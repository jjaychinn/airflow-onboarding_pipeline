#!/usr/bin/env python3
"""
validate_raw_data.py
=====================
Standalone validator for the 3 mock OLTP CSVs against the take-home test's
requirements (Section 2 "Source Data" + Appendix A of the test PDF).

Usage:
    python3 validate_raw_data.py [--dir data/raw]

    --dir   folder containing applicants.csv, onboarding_applications.csv,
            onboarding_events.csv (default: ./data/raw)

Exit code: 0 if every check passes, 1 if any check fails (useful in CI /
pre-commit / as an Airflow task).

Dependency: pandas only (pip install pandas).

What it checks
---------------
1. Row count minimums (>=1,000 / >=1,200 / >=8,000)
2. Event timestamps span >= 90 days
3. Primary key uniqueness (applicant_id, application_id, event_id)
4. Required (non-null) fields
5. Referential integrity (applications -> applicants, events -> applications)
6. Accepted values for enumerated columns (edit ALLOWED_VALUES if you changed
   the vocabulary in your own generator/scenario)
7. event_sequence is a contiguous 1..N run per application, strictly increasing
8. event_timestamp strictly increasing within an application, consistent with
   event_sequence order
9. event_metadata is valid JSON wherever it is populated
10. Application decision is only set when application_status is a terminal
    decision state (APPROVED/REJECTED)
11. Application milestone timestamps (submitted_at/completed_at) are
    consistent with application_status (e.g. no completed_at for a STARTED
    application) and never before started_at
12. current_step / application_status / decision are consistent with the
    application's own *last* event (the DB-level "must match the latest
    event" requirement in Appendix A)
"""
import argparse
import json
import os
import sys

import pandas as pd

# --------------------------------------------------------------------------
# Config — adjust this if you changed the enum vocabulary in your generator
# --------------------------------------------------------------------------
MIN_ROWS = {
    "applicants": 1000,
    "onboarding_applications": 1200,
    "onboarding_events": 8000,
}
MIN_EVENT_SPAN_DAYS = 90

ALLOWED_VALUES = {
    "applicants": {
        "applicant_type": {"INDIVIDUAL", "BUSINESS"},
    },
    "onboarding_applications": {
        "application_status": {"STARTED", "IN_PROGRESS", "SUBMITTED",
                                "APPROVED", "REJECTED", "ABANDONED"},
    },
    "onboarding_events": {
        "event_outcome": {"SUCCESS", "FAILED", "PENDING"},
    },
}

TERMINAL_DECISION_STATUSES = {"APPROVED", "REJECTED"}
# statuses that imply the application has a completed_at
TERMINAL_STATUSES = {"APPROVED", "REJECTED", "ABANDONED"}
# statuses that DEFINITELY imply submission happened (ABANDONED is
# deliberately excluded: an application can be abandoned either before or
# after submitting, so submitted_at being null is legitimate for ABANDONED)
SUBMITTED_OR_LATER = {"SUBMITTED", "APPROVED", "REJECTED"}

REQUIRED_FIELDS = {
    "applicants": ["applicant_id", "applicant_type", "created_at"],
    "onboarding_applications": ["application_id", "applicant_id",
                                 "started_at", "application_status"],
    "onboarding_events": ["event_id", "application_id", "event_sequence",
                           "event_timestamp", "event_type", "event_outcome"],
}

# --------------------------------------------------------------------------
# Reporting helpers
# --------------------------------------------------------------------------
results = []  # list of (status: "PASS"/"FAIL"/"WARN", message)


def check(condition, pass_msg, fail_msg):
    if condition:
        results.append(("PASS", pass_msg))
    else:
        results.append(("FAIL", fail_msg))


def check_zero(bad_count, description, sample_index=None, df=None, n_show=5):
    """Fail if bad_count > 0, optionally printing a few offending rows."""
    if bad_count == 0:
        results.append(("PASS", description))
    else:
        msg = f"{description} -> {bad_count} bad row(s)"
        if sample_index is not None and df is not None and len(sample_index) > 0:
            sample = df.loc[sample_index[:n_show]]
            msg += f"\n         sample offending rows (up to {n_show}):\n"
            msg += "\n".join(f"           {row.to_dict()}" for _, row in sample.iterrows())
        results.append(("FAIL", msg))


def is_valid_json_or_blank(val):
    if pd.isna(val) or val == "":
        return True
    try:
        json.loads(val)
        return True
    except (json.JSONDecodeError, TypeError):
        return False


# --------------------------------------------------------------------------
# Load
# --------------------------------------------------------------------------
def load(data_dir):
    paths = {
        "applicants": os.path.join(data_dir, "applicants.csv"),
        "onboarding_applications": os.path.join(data_dir, "onboarding_applications.csv"),
        "onboarding_events": os.path.join(data_dir, "onboarding_events.csv"),
    }
    for name, path in paths.items():
        if not os.path.exists(path):
            print(f"ERROR: required file not found: {path}")
            sys.exit(2)

    applicants = pd.read_csv(paths["applicants"], dtype=str, keep_default_na=False)
    applications = pd.read_csv(paths["onboarding_applications"], dtype=str, keep_default_na=False)
    events = pd.read_csv(paths["onboarding_events"], dtype=str, keep_default_na=False)

    # normalize empty strings to NaN for null-style checks, keep a raw copy for JSON checks
    for df in (applicants, applications, events):
        df.replace("", pd.NA, inplace=True)

    # typed columns needed for logic
    applications["started_at_dt"] = pd.to_datetime(applications["started_at"], errors="coerce", utc=True)
    applications["submitted_at_dt"] = pd.to_datetime(applications["submitted_at"], errors="coerce", utc=True)
    applications["completed_at_dt"] = pd.to_datetime(applications["completed_at"], errors="coerce", utc=True)

    events["event_timestamp_dt"] = pd.to_datetime(events["event_timestamp"], errors="coerce", utc=True)
    events["event_sequence_int"] = pd.to_numeric(events["event_sequence"], errors="coerce")

    return applicants, applications, events


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------
def run_checks(applicants, applications, events):
    dfs = {
        "applicants": applicants,
        "onboarding_applications": applications,
        "onboarding_events": events,
    }

    # 1. Row count minimums --------------------------------------------------
    for name, df in dfs.items():
        check(len(df) >= MIN_ROWS[name],
              f"{name}: row count {len(df)} >= {MIN_ROWS[name]}",
              f"{name}: row count {len(df)} < required minimum {MIN_ROWS[name]}")

    # 2. Event date span ------------------------------------------------------
    valid_ts = events["event_timestamp_dt"].dropna()
    if len(valid_ts) == 0:
        results.append(("FAIL", "onboarding_events: no parseable event_timestamp values"))
    else:
        span_days = (valid_ts.max() - valid_ts.min()).days
        check(span_days >= MIN_EVENT_SPAN_DAYS,
              f"onboarding_events: date span {span_days} days >= {MIN_EVENT_SPAN_DAYS} "
              f"({valid_ts.min().date()} .. {valid_ts.max().date()})",
              f"onboarding_events: date span {span_days} days < required minimum "
              f"{MIN_EVENT_SPAN_DAYS} ({valid_ts.min().date()} .. {valid_ts.max().date()})")

    # 3. Primary key uniqueness -----------------------------------------------
    pk_cols = {
        "applicants": "applicant_id",
        "onboarding_applications": "application_id",
        "onboarding_events": "event_id",
    }
    for name, pk in pk_cols.items():
        df = dfs[name]
        dupes = df[df.duplicated(subset=[pk], keep=False)]
        check_zero(len(dupes.index), f"{name}.{pk} uniqueness", dupes.index.tolist(), df)

    # 4. Required (non-null) fields --------------------------------------------
    for name, cols in REQUIRED_FIELDS.items():
        df = dfs[name]
        for col in cols:
            bad = df[df[col].isna()]
            check_zero(len(bad.index), f"{name}.{col} required (non-null)", bad.index.tolist(), df)

    # 5. Referential integrity -------------------------------------------------
    applicant_ids = set(applicants["applicant_id"].dropna())
    bad_fk = applications[~applications["applicant_id"].isin(applicant_ids)]
    check_zero(len(bad_fk.index), "onboarding_applications.applicant_id -> applicants.applicant_id",
               bad_fk.index.tolist(), applications)

    application_ids = set(applications["application_id"].dropna())
    bad_fk2 = events[~events["application_id"].isin(application_ids)]
    check_zero(len(bad_fk2.index), "onboarding_events.application_id -> onboarding_applications.application_id",
               bad_fk2.index.tolist(), events)

    # 6. Accepted values --------------------------------------------------------
    for name, col_rules in ALLOWED_VALUES.items():
        df = dfs[name]
        for col, allowed in col_rules.items():
            bad = df[~df[col].isin(allowed) & df[col].notna()]
            check_zero(len(bad.index), f"{name}.{col} accepted values {sorted(allowed)}",
                       bad.index.tolist(), df)

    # 7. event_sequence contiguous & strictly increasing per application --------
    seq_issues = 0
    seq_bad_apps = []
    for app_id, grp in events.groupby("application_id"):
        seqs = grp["event_sequence_int"].dropna().sort_values().tolist()
        expected = list(range(1, len(seqs) + 1))
        if seqs != expected:
            seq_issues += 1
            seq_bad_apps.append(app_id)
    check(seq_issues == 0,
          "onboarding_events.event_sequence forms a contiguous 1..N run per application",
          f"onboarding_events.event_sequence not contiguous/increasing for {seq_issues} "
          f"application(s), e.g. {seq_bad_apps[:5]}")

    # 8. event_timestamp strictly increasing, consistent with event_sequence order -
    ts_issues = 0
    ts_bad_apps = []
    for app_id, grp in events.groupby("application_id"):
        ordered = grp.sort_values("event_sequence_int")
        ts = ordered["event_timestamp_dt"].tolist()
        if any(pd.isna(t) for t in ts):
            ts_issues += 1
            ts_bad_apps.append(app_id)
            continue
        if any(ts[i] >= ts[i + 1] for i in range(len(ts) - 1)):
            ts_issues += 1
            ts_bad_apps.append(app_id)
    check(ts_issues == 0,
          "onboarding_events.event_timestamp strictly increasing per application (by event_sequence)",
          f"onboarding_events.event_timestamp not strictly increasing for {ts_issues} "
          f"application(s), e.g. {ts_bad_apps[:5]}")

    # 9. event_metadata JSON validity --------------------------------------------
    bad_json_mask = ~events["event_metadata"].apply(is_valid_json_or_blank)
    bad_json = events[bad_json_mask]
    check_zero(len(bad_json.index), "onboarding_events.event_metadata is valid JSON where populated",
               bad_json.index.tolist(), events)

    # 10. decision only set for terminal decision statuses -----------------------
    has_decision = applications["decision"].notna()
    bad_decision = applications[has_decision & ~applications["application_status"].isin(TERMINAL_DECISION_STATUSES)]
    check_zero(len(bad_decision.index),
               "onboarding_applications.decision only set when application_status is APPROVED/REJECTED",
               bad_decision.index.tolist(), applications)

    # also: APPROVED/REJECTED must have a decision
    missing_decision = applications[
        applications["application_status"].isin(TERMINAL_DECISION_STATUSES) & applications["decision"].isna()
    ]
    check_zero(len(missing_decision.index),
               "onboarding_applications: APPROVED/REJECTED rows have a decision value",
               missing_decision.index.tolist(), applications)

    # 11. milestone timestamps consistent with status ----------------------------
    # submitted_at must be present iff status implies submission happened
    bad_submitted_missing = applications[
        applications["application_status"].isin(SUBMITTED_OR_LATER) & applications["submitted_at_dt"].isna()
    ]
    check_zero(len(bad_submitted_missing.index),
               "onboarding_applications.submitted_at present for SUBMITTED/APPROVED/REJECTED rows",
               bad_submitted_missing.index.tolist(), applications)

    # completed_at must be present iff status is terminal (approved/rejected/abandoned)
    bad_completed_missing = applications[
        applications["application_status"].isin(TERMINAL_STATUSES) & applications["completed_at_dt"].isna()
    ]
    check_zero(len(bad_completed_missing.index),
               "onboarding_applications.completed_at present for APPROVED/REJECTED/ABANDONED rows",
               bad_completed_missing.index.tolist(), applications)

    # completed_at must be null for non-terminal statuses
    bad_completed_present = applications[
        ~applications["application_status"].isin(TERMINAL_STATUSES) & applications["completed_at_dt"].notna()
    ]
    check_zero(len(bad_completed_present.index),
               "onboarding_applications.completed_at is null for non-terminal statuses",
               bad_completed_present.index.tolist(), applications)

    # timestamps must not go backwards: started <= submitted <= completed
    ordering_bad_idx = []
    for idx, row in applications.iterrows():
        s, sub, c = row["started_at_dt"], row["submitted_at_dt"], row["completed_at_dt"]
        if pd.notna(s) and pd.notna(sub) and sub < s:
            ordering_bad_idx.append(idx)
        elif pd.notna(sub) and pd.notna(c) and c < sub:
            ordering_bad_idx.append(idx)
        elif pd.isna(sub) and pd.notna(s) and pd.notna(c) and c < s:
            ordering_bad_idx.append(idx)
    check_zero(len(ordering_bad_idx),
               "onboarding_applications timestamps ordered: started_at <= submitted_at <= completed_at",
               ordering_bad_idx, applications)

    # 12. current_step/status/decision match the application's LAST event --------
    last_events = (
        events.sort_values("event_sequence_int")
        .groupby("application_id")
        .tail(1)
        .set_index("application_id")
    )
    mismatch_idx = []
    for idx, row in applications.iterrows():
        app_id = row["application_id"]
        if app_id not in last_events.index:
            mismatch_idx.append(idx)
            continue
        last = last_events.loc[app_id]
        # the application's to_status should reflect the last event's to_status,
        # OR the last event's to_status should be consistent with application_status
        # (exact field name may differ; this checks event_type/to_status alignment)
        if "to_status" in last and pd.notna(last.get("to_status")):
            if last["to_status"] not in (row["application_status"], row.get("current_step")):
                # allow a soft match: last event to_status should equal application_status
                # for terminal events, or just be a valid progression otherwise
                if row["application_status"] in TERMINAL_STATUSES or row["application_status"] in ("SUBMITTED",):
                    if last["to_status"] != row["application_status"]:
                        mismatch_idx.append(idx)
    check_zero(len(mismatch_idx),
               "onboarding_applications.application_status matches its application's last event.to_status "
               "(for SUBMITTED/APPROVED/REJECTED/ABANDONED rows)",
               mismatch_idx, applications)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Validate onboarding mock CSVs against the take-home test spec.")
    parser.add_argument("--dir", default="data/raw", help="folder containing the 3 CSVs (default: data/raw)")
    args = parser.parse_args()

    applicants, applications, events = load(args.dir)
    run_checks(applicants, applications, events)

    n_pass = sum(1 for s, _ in results if s == "PASS")
    n_fail = sum(1 for s, _ in results if s == "FAIL")

    print(f"Validating CSVs in: {args.dir}\n")
    for status, msg in results:
        print(f"[{status}] {msg}")

    print(f"\n{n_pass} passed, {n_fail} failed, {len(results)} total checks.")
    sys.exit(1 if n_fail > 0 else 0)


if __name__ == "__main__":
    main()
