"""
Onboarding Data Pipeline
=========================
Raw CSV -> DuckDB raw layer (No change) -> staging (Change data type) -> dimensional model -> data
quality tests -> Parquet analytical outputs.

All heavy lifting runs inside DuckDB (single-file warehouse). Airflow's job
here is only orchestration: each task opens its own DuckDB connection against
the same on-disk file, so tasks must run sequentially (no parallel writers).
"""
from __future__ import annotations

import os
from datetime import datetime

import duckdb
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# param
DATA_DIR = os.environ.get("ONBOARDING_DATA_DIR", "/opt/airflow/data")
RAW_DIR = f"{DATA_DIR}/raw"                 # mount your generated CSVs here
DB_PATH = f"{DATA_DIR}/warehouse/warehouse.duckdb"
OUTPUT_DIR = f"{DATA_DIR}/output"

RAW_FILES = {
    "applicants": f"{RAW_DIR}/applicants.csv",
    "onboarding_applications": f"{RAW_DIR}/onboarding_applications.csv",
    "onboarding_events": f"{RAW_DIR}/onboarding_events.csv",
}

# connection and assertions
def get_conn() -> duckdb.DuckDBPyConnection:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    return duckdb.connect(DB_PATH)


def run_assertions(con: duckdb.DuckDBPyConnection, assertions: list[tuple[str, str]]) -> None:
    """Each assertion is (description, sql). SQL must return a single row/
    single column count of FAILING records — 0 means pass."""
    failures = []
    for description, sql in assertions:
        bad_count = con.execute(sql).fetchone()[0]
        if bad_count and bad_count > 0:
            failures.append(f"  - [{bad_count} rows] {description}")
    if failures:
        raise AirflowException("Data quality checks failed:\n" + "\n".join(failures))


# DAG
@dag(
    dag_id="onboarding_pipeline",
    description="Ingest onboarding OLTP CSVs, model as a star schema in DuckDB, export Parquet marts",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,      # DuckDB allows a single writer
    tags=["onboarding", "duckdb", "dimensional-model"],
)
def onboarding_pipeline():

    @task
    def ingest_raw() -> str:
        """Load the 3 CSVs into raw.* tables with NO transformation.
        Source values are kept as-is (read as VARCHAR) so raw is a faithful
        copy of the source system."""
        con = get_conn()
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        for table, path in RAW_FILES.items():
            con.execute(f"""
                CREATE OR REPLACE TABLE raw.{table} AS
                SELECT * FROM read_csv_auto('{path}', ALL_VARCHAR=TRUE, header=True)
            """)
            n = con.execute(f"SELECT COUNT(*) FROM raw.{table}").fetchone()[0]
            print(f"raw.{table}: {n} rows loaded from {path}")
        con.close()
        return DB_PATH

    @task
    def build_staging():
        """Cast raw VARCHAR columns to proper types. Still 1:1 grain with source,
        just typed and lightly cleaned (null-ify empty strings)."""
        con = get_conn()
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")

        con.execute("""
            CREATE OR REPLACE TABLE staging.stg_applicants AS
            SELECT
                applicant_id,
                applicant_type,
                country_code,
                region,
                acquisition_channel,
                NULLIF(marketing_campaign, '') AS marketing_campaign,
                preferred_language,
                CAST(created_at AS TIMESTAMP) AS created_at,
                CAST(updated_at AS TIMESTAMP) AS updated_at
            FROM raw.applicants
        """)

        con.execute("""
            CREATE OR REPLACE TABLE staging.stg_applications AS
            SELECT
                application_id,
                applicant_id,
                application_type,
                source_channel,
                device_type,
                CAST(started_at AS TIMESTAMP) AS started_at,
                CAST(NULLIF(submitted_at, '') AS TIMESTAMP) AS submitted_at,
                CAST(NULLIF(completed_at, '') AS TIMESTAMP) AS completed_at,
                current_step,
                application_status,
                NULLIF(decision, '') AS decision,
                NULLIF(rejection_reason, '') AS rejection_reason,
                CAST(updated_at AS TIMESTAMP) AS updated_at
            FROM raw.onboarding_applications
        """)

        con.execute("""
            CREATE OR REPLACE TABLE staging.stg_events AS
            SELECT
                event_id,
                application_id,
                CAST(event_sequence AS INTEGER) AS event_sequence,
                CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
                event_type,
                step_name,
                NULLIF(from_status, '') AS from_status,
                to_status,
                event_outcome,
                NULLIF(failure_reason, '') AS failure_reason,
                session_id,
                source_channel,
                NULLIF(event_metadata, '') AS event_metadata
            FROM raw.onboarding_events
        """)
        con.close()

    @task
    def test_staging():
        """Source-level data quality gates before anything is modeled."""
        con = get_conn()
        assertions = [
            # --- check value possible ---
            ("stg_applicants.applicant_id not unique",
             "SELECT COUNT(*) FROM (SELECT applicant_id FROM staging.stg_applicants GROUP BY 1 HAVING COUNT(*) > 1)"),
            ("stg_applications.application_id not unique",
             "SELECT COUNT(*) FROM (SELECT application_id FROM staging.stg_applications GROUP BY 1 HAVING COUNT(*) > 1)"),
            ("stg_events.event_id not unique",
             "SELECT COUNT(*) FROM (SELECT event_id FROM staging.stg_events GROUP BY 1 HAVING COUNT(*) > 1)"),
            ("stg_applications.applicant_id orphaned (no parent applicant)",
             """SELECT COUNT(*) FROM staging.stg_applications a
                LEFT JOIN staging.stg_applicants p ON a.applicant_id = p.applicant_id
                WHERE p.applicant_id IS NULL"""),
            ("stg_events.application_id orphaned (no parent application)",
             """SELECT COUNT(*) FROM staging.stg_events e
                LEFT JOIN staging.stg_applications a ON e.application_id = a.application_id
                WHERE a.application_id IS NULL"""),
            ("application_status has unexpected value",
             """SELECT COUNT(*) FROM staging.stg_applications
                WHERE application_status NOT IN
                ('STARTED','IN_PROGRESS','SUBMITTED','APPROVED','REJECTED','ABANDONED')"""),
            ("event_outcome has unexpected value",
             "SELECT COUNT(*) FROM staging.stg_events WHERE event_outcome NOT IN ('SUCCESS','FAILED','PENDING')"),
            ("applicant_type has unexpected value",
             "SELECT COUNT(*) FROM staging.stg_applicants WHERE applicant_type NOT IN ('INDIVIDUAL','BUSINESS')"),
            ("application_type has unexpected value",
             "SELECT COUNT(*) FROM staging.stg_applications WHERE application_type NOT IN ('STANDARD','PREMIUM','BUSINESS','PARTNER')"),
            ("source_channel has unexpected value",
             "SELECT COUNT(*) FROM staging.stg_applications WHERE source_channel NOT IN ('WEB','MOBILE','PARTNER','ASSISTED')"),
            ("device_type has unexpected value",
             "SELECT COUNT(*) FROM staging.stg_applications WHERE device_type NOT IN ('DESKTOP','MOBILE','TABLET','OTHER')"),
            ("event_type has unexpected value",
             """SELECT COUNT(*) FROM staging.stg_events
                WHERE event_type NOT IN ('APPLICATION_CREATED','STEP_STARTED','STEP_COMPLETED','STEP_FAILED',
                                         'STEP_RETRIED','SESSION_RESUMED','DECISION_RECORDED','APPLICATION_ABANDONED')"""),
            ("event_sequence not strictly increasing within an application",
             """SELECT COUNT(*) FROM (
                    SELECT application_id, event_sequence,
                           LAG(event_sequence) OVER (PARTITION BY application_id ORDER BY event_timestamp, event_sequence) AS prev_seq
                    FROM staging.stg_events
                ) WHERE prev_seq IS NOT NULL AND event_sequence <= prev_seq"""),
            ("event_timestamp not increasing within an application",
             """SELECT COUNT(*) FROM (
                    SELECT application_id, event_timestamp,
                           LAG(event_timestamp) OVER (PARTITION BY application_id ORDER BY event_sequence) AS prev_ts
                    FROM staging.stg_events
                ) WHERE prev_ts IS NOT NULL AND event_timestamp <= prev_ts"""),
            ("event_metadata contains invalid JSON",
             """SELECT COUNT(*) FROM staging.stg_events
                WHERE event_metadata IS NOT NULL AND NOT json_valid(event_metadata)"""),
            ("decision/status mismatch (decision set but status not terminal)",
             """SELECT COUNT(*) FROM staging.stg_applications
                WHERE decision IS NOT NULL AND application_status NOT IN ('APPROVED','REJECTED')"""),

            # --- required fields (Appendix A "Required = Yes") ---
            ("required field null in stg_applicants",
             """SELECT COUNT(*) FROM staging.stg_applicants
                WHERE applicant_id IS NULL OR applicant_type IS NULL OR country_code IS NULL OR region IS NULL
                   OR acquisition_channel IS NULL OR preferred_language IS NULL
                   OR created_at IS NULL OR updated_at IS NULL"""),
            ("required field null in stg_applications",
             """SELECT COUNT(*) FROM staging.stg_applications
                WHERE application_id IS NULL OR applicant_id IS NULL OR application_type IS NULL
                   OR source_channel IS NULL OR device_type IS NULL OR started_at IS NULL
                   OR current_step IS NULL OR application_status IS NULL OR updated_at IS NULL"""),
            ("required field null in stg_events",
             """SELECT COUNT(*) FROM staging.stg_events
                WHERE event_id IS NULL OR application_id IS NULL OR event_sequence IS NULL
                   OR event_timestamp IS NULL OR event_type IS NULL OR step_name IS NULL OR to_status IS NULL
                   OR event_outcome IS NULL OR session_id IS NULL OR source_channel IS NULL"""),

            # --- state transitions ---
            ("first event is not APPLICATION_CREATED (NULL -> STARTED)",
             """SELECT COUNT(*) FROM staging.stg_events
                WHERE event_sequence = 1
                  AND (event_type <> 'APPLICATION_CREATED' OR from_status IS NOT NULL OR to_status <> 'STARTED')"""),
            ("from_status does not equal the previous event's to_status",
             """SELECT COUNT(*) FROM (
                    SELECT from_status,
                           LAG(to_status) OVER (PARTITION BY application_id ORDER BY event_sequence) AS prev_to_status
                    FROM staging.stg_events
                ) WHERE prev_to_status IS NOT NULL AND from_status IS DISTINCT FROM prev_to_status"""),
            ("status transition not allowed",
             """SELECT COUNT(*) FROM staging.stg_events
                WHERE event_sequence > 1
                  AND (from_status, to_status) NOT IN (
                      ('STARTED','IN_PROGRESS'), ('STARTED','ABANDONED'),
                      ('IN_PROGRESS','IN_PROGRESS'), ('IN_PROGRESS','SUBMITTED'), ('IN_PROGRESS','ABANDONED'),
                      ('SUBMITTED','APPROVED'), ('SUBMITTED','REJECTED'), ('SUBMITTED','ABANDONED'))"""),
            ("event recorded after a terminal status",
             """SELECT COUNT(*) FROM (
                    SELECT LAG(to_status) OVER (PARTITION BY application_id ORDER BY event_sequence) AS prev_to_status
                    FROM staging.stg_events
                ) WHERE prev_to_status IN ('APPROVED','REJECTED','ABANDONED')"""),

            # --- application must match its latest events ---
            ("application status / current_step / updated_at does not match its latest event",
             """SELECT COUNT(*) FROM staging.stg_applications a
                JOIN (SELECT * FROM staging.stg_events
                      QUALIFY ROW_NUMBER() OVER (PARTITION BY application_id ORDER BY event_sequence DESC) = 1
                     ) l ON a.application_id = l.application_id
                WHERE a.application_status <> l.to_status
                   OR a.current_step <> l.step_name
                   OR a.updated_at <> l.event_timestamp"""),
            ("submitted_at / completed_at does not match the SUBMITTED / terminal event",
             """SELECT COUNT(*) FROM staging.stg_applications a
                JOIN (SELECT application_id,
                             MIN(CASE WHEN to_status = 'SUBMITTED' THEN event_timestamp END) AS submitted_ts,
                             MIN(CASE WHEN to_status IN ('APPROVED','REJECTED','ABANDONED')
                                      THEN event_timestamp END) AS terminal_ts
                      FROM staging.stg_events GROUP BY application_id
                     ) m ON a.application_id = m.application_id
                WHERE a.submitted_at IS DISTINCT FROM m.submitted_ts
                   OR a.completed_at IS DISTINCT FROM m.terminal_ts"""),

            # --- timestamps ---
            ("milestones out of order (started <= submitted <= completed <= updated)",
             """SELECT COUNT(*) FROM staging.stg_applications
                WHERE submitted_at < started_at OR completed_at < started_at
                   OR completed_at < submitted_at OR updated_at < started_at"""),
        ]
        run_assertions(con, assertions)
        con.close()

    @task
    def build_dimensional_model():
        """
        Star schema (business process: applicant onboarding funnel):

          dim_date            - calendar, one row per day
          dim_applicant        - SCD1 snapshot of current applicant attributes
          dim_channel          - conformed channel codes (source + acquisition channels)
          dim_application       - one row per application, descriptive attrs
          fact_onboarding_events - grain: one row per onboarding event (transaction fact)
          fact_application_snapshot - grain: one row per application (accumulating
                                       snapshot fact with milestone dates + durations)

        Unknown handling: facts LEFT JOIN the dimensions and fall back to -1;
        dim_applicant and dim_channel carry a -1 "Unknown" member row, so a
        missing lookup never drops a fact row. Orphaned sources (e.g. an event
        without its application) are already stopped by test_staging.
        """
        con = get_conn()
        con.execute("CREATE SCHEMA IF NOT EXISTS marts")

        # --- dim_date ---------------------------------------------------
        con.execute("""
            CREATE OR REPLACE TABLE marts.dim_date AS
            WITH RECURSIVE
            bounds AS (
                -- events run past the last started_at, so bound by event dates too
                SELECT MIN(CAST(event_timestamp AS DATE)) AS min_d,
                       MAX(CAST(event_timestamp AS DATE)) AS max_d
                FROM staging.stg_events
            ),
            date_series AS (
                SELECT min_d AS d FROM bounds
                UNION ALL
                SELECT (ds.d + INTERVAL 1 DAY)::DATE
                FROM date_series ds, bounds b
                WHERE ds.d < b.max_d
            )
            SELECT
                CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
                d AS calendar_date,
                extract(year FROM d) AS year,
                extract(month FROM d) AS month,
                extract(day FROM d) AS day,
                extract(dow FROM d) AS day_of_week,
                strftime(d, '%A') AS day_name
            FROM date_series
        """)

        # --- dim_channel (conformed dimension, shared by app + event facts) ---
        con.execute("""
            CREATE OR REPLACE TABLE marts.dim_channel AS
            SELECT -1 AS channel_key, 'UNKNOWN' AS channel_code
            UNION ALL
            SELECT ROW_NUMBER() OVER (ORDER BY channel_code) AS channel_key,
                   channel_code
            FROM (
                SELECT DISTINCT source_channel AS channel_code FROM staging.stg_applications
                UNION
                SELECT DISTINCT acquisition_channel FROM staging.stg_applicants
                UNION
                SELECT DISTINCT source_channel FROM staging.stg_events
            ) sub
            WHERE channel_code IS NOT NULL
        """)

        # --- dim_applicant (SCD1: current attributes only) ---------------
        con.execute("""
            CREATE OR REPLACE TABLE marts.dim_applicant AS
            SELECT -1 AS applicant_key, 'UNKNOWN' AS applicant_id, NULL AS applicant_type,
                   NULL AS country_code, NULL AS region, NULL AS acquisition_channel,
                   NULL AS marketing_campaign, NULL AS preferred_language
            UNION ALL
            SELECT ROW_NUMBER() OVER (ORDER BY applicant_id) AS applicant_key,
                   applicant_id, applicant_type, country_code, region,
                   acquisition_channel, marketing_campaign, preferred_language
            FROM staging.stg_applicants
        """)

        # --- dim_application (descriptive attrs, one row per application) --
        con.execute("""
            CREATE OR REPLACE TABLE marts.dim_application AS
            SELECT ROW_NUMBER() OVER (ORDER BY application_id) AS application_key,
                   application_id, application_type, device_type, current_step
            FROM staging.stg_applications
        """)

        # --- fact_onboarding_events (transaction fact, grain = 1 event) --
        con.execute("""
            CREATE OR REPLACE TABLE marts.fact_onboarding_events AS
            SELECT
                e.event_id,
                COALESCE(da.application_key, -1) AS application_key,
                COALESCE(dc.channel_key, -1) AS channel_key,
                CAST(strftime(e.event_timestamp, '%Y%m%d') AS INTEGER) AS date_key,
                e.event_sequence,
                e.event_timestamp,
                e.event_type,
                e.step_name,
                e.from_status,
                e.to_status,
                e.event_outcome,
                e.failure_reason,
                e.session_id,
                e.event_metadata,
                -- seconds since the previous event on the same application (0 for first event)
                COALESCE(date_diff('second',
                    LAG(e.event_timestamp) OVER (PARTITION BY e.application_id ORDER BY e.event_sequence),
                    e.event_timestamp), 0) AS seconds_since_prev_event
            FROM staging.stg_events e
            LEFT JOIN marts.dim_application da ON e.application_id = da.application_id
            LEFT JOIN marts.dim_channel dc ON e.source_channel = dc.channel_code
        """)

        # --- fact_application_snapshot (accumulating snapshot, grain = 1 application) ---
        con.execute("""
            CREATE OR REPLACE TABLE marts.fact_application_snapshot AS
            SELECT
                a.application_id,
                COALESCE(da.application_key, -1) AS application_key,
                COALESCE(dap.applicant_key, -1) AS applicant_key,
                COALESCE(dc.channel_key, -1) AS channel_key,
                CAST(strftime(a.started_at, '%Y%m%d') AS INTEGER) AS started_date_key,
                CAST(strftime(a.submitted_at, '%Y%m%d') AS INTEGER) AS submitted_date_key,
                CAST(strftime(a.completed_at, '%Y%m%d') AS INTEGER) AS completed_date_key,
                a.started_at,
                a.submitted_at,
                a.completed_at,
                a.application_status,
                a.decision,
                a.rejection_reason,
                date_diff('second', a.started_at, a.submitted_at) AS seconds_to_submit,
                date_diff('second', a.started_at, a.completed_at) AS seconds_to_complete,
                (SELECT COUNT(*) FROM staging.stg_events ev
                    WHERE ev.application_id = a.application_id AND ev.event_outcome = 'FAILED') AS failure_event_count,
                (SELECT COUNT(DISTINCT ev.session_id) FROM staging.stg_events ev
                    WHERE ev.application_id = a.application_id) AS session_count
            FROM staging.stg_applications a
            LEFT JOIN marts.dim_application da ON a.application_id = da.application_id
            LEFT JOIN marts.dim_applicant dap ON a.applicant_id = dap.applicant_id
            LEFT JOIN marts.dim_channel dc ON a.source_channel = dc.channel_code
        """)
        con.close()

    @task
    def test_marts():
        con = get_conn()
        assertions = [
            ("fact_application_snapshot grain violated (application_id not unique)",
             "SELECT COUNT(*) FROM (SELECT application_id FROM marts.fact_application_snapshot GROUP BY 1 HAVING COUNT(*) > 1)"),
            ("fact_onboarding_events.application_key not in dim_application (incl. -1 Unknown)",
             """SELECT COUNT(*) FROM marts.fact_onboarding_events f
                LEFT JOIN marts.dim_application d ON f.application_key = d.application_key
                WHERE f.application_key != -1 AND d.application_key IS NULL"""),
            ("fact_application_snapshot row count != stg_applications row count",
             """SELECT ABS(
                    (SELECT COUNT(*) FROM marts.fact_application_snapshot) -
                    (SELECT COUNT(*) FROM staging.stg_applications))"""),
            ("negative duration seconds_to_complete",
             "SELECT COUNT(*) FROM marts.fact_application_snapshot WHERE seconds_to_complete < 0"),
            ("fact_onboarding_events grain violated (event_id not unique)",
             "SELECT COUNT(*) FROM (SELECT event_id FROM marts.fact_onboarding_events GROUP BY 1 HAVING COUNT(*) > 1)"),
            ("fact_onboarding_events row count != stg_events row count",
             """SELECT ABS(
                    (SELECT COUNT(*) FROM marts.fact_onboarding_events) -
                    (SELECT COUNT(*) FROM staging.stg_events))"""),
            ("dimension key not unique",
             """SELECT (SELECT COUNT(*) - COUNT(DISTINCT applicant_key) FROM marts.dim_applicant)
                     + (SELECT COUNT(*) - COUNT(DISTINCT application_key) FROM marts.dim_application)
                     + (SELECT COUNT(*) - COUNT(DISTINCT channel_key) FROM marts.dim_channel)
                     + (SELECT COUNT(*) - COUNT(DISTINCT date_key) FROM marts.dim_date)"""),
            ("date key not found in dim_date",
             """SELECT (SELECT COUNT(*) FROM marts.fact_onboarding_events
                        WHERE date_key NOT IN (SELECT date_key FROM marts.dim_date))
                     + (SELECT COUNT(*) FROM marts.fact_application_snapshot
                        WHERE started_date_key NOT IN (SELECT date_key FROM marts.dim_date)
                           OR submitted_date_key NOT IN (SELECT date_key FROM marts.dim_date)
                           OR completed_date_key NOT IN (SELECT date_key FROM marts.dim_date))"""),
        ]
        run_assertions(con, assertions)
        con.close()

    @task
    def export_parquet():
        """At least 2 analytical Parquet outputs."""
        con = get_conn()
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Output 1: onboarding funnel by date / channel / application type
        con.execute(f"""
            COPY (
                SELECT
                    d.calendar_date,
                    dc.channel_code,
                    da.application_type,
                    COUNT(DISTINCT f.application_id) AS applications_started,
                    -- cumulative: approved/rejected applications were submitted too
                    COUNT(DISTINCT CASE WHEN f.submitted_at IS NOT NULL
                        THEN f.application_id END) AS applications_submitted,
                    COUNT(DISTINCT CASE WHEN f.application_status = 'APPROVED'
                        THEN f.application_id END) AS applications_approved,
                    COUNT(DISTINCT CASE WHEN f.application_status = 'REJECTED'
                        THEN f.application_id END) AS applications_rejected,
                    COUNT(DISTINCT CASE WHEN f.application_status = 'ABANDONED'
                        THEN f.application_id END) AS applications_abandoned
                FROM marts.fact_application_snapshot f
                JOIN marts.dim_date d ON f.started_date_key = d.date_key
                JOIN marts.dim_channel dc ON f.channel_key = dc.channel_key
                JOIN marts.dim_application da ON f.application_key = da.application_key
                GROUP BY 1, 2, 3
                ORDER BY 1, 2, 3
            ) TO '{OUTPUT_DIR}/onboarding_funnel_by_date_channel.parquet' (FORMAT PARQUET)
        """)

        # Output 2: completion duration + failure/session stats by channel
        con.execute(f"""
            COPY (
                SELECT
                    dc.channel_code,
                    da.application_type,
                    COUNT(*) AS n_applications,
                    AVG(f.seconds_to_submit) / 60.0 AS avg_minutes_to_submit,
                    -- decided applications only (abandoned ones would mix in time-to-abandon)
                    AVG(CASE WHEN f.decision IS NOT NULL THEN f.seconds_to_complete END) / 60.0
                        AS avg_minutes_to_decision,
                    AVG(f.failure_event_count) AS avg_failure_events,
                    AVG(f.session_count) AS avg_sessions,
                    SUM(CASE WHEN f.application_status = 'ABANDONED' THEN 1 ELSE 0 END) * 1.0
                        / COUNT(*) AS abandonment_rate
                FROM marts.fact_application_snapshot f
                JOIN marts.dim_channel dc ON f.channel_key = dc.channel_key
                JOIN marts.dim_application da ON f.application_key = da.application_key
                GROUP BY 1, 2
                ORDER BY 1, 2
            ) TO '{OUTPUT_DIR}/onboarding_duration_and_failure_by_channel.parquet' (FORMAT PARQUET)
        """)

        for f in ["onboarding_funnel_by_date_channel.parquet",
                  "onboarding_duration_and_failure_by_channel.parquet"]:
            n = con.execute(f"SELECT COUNT(*) FROM '{OUTPUT_DIR}/{f}'").fetchone()[0]
            print(f"{f}: {n} rows written")
        con.close()

    (
        ingest_raw()
        >> build_staging()
        >> test_staging()
        >> build_dimensional_model()
        >> test_marts()
        >> export_parquet()
    )


onboarding_pipeline()
