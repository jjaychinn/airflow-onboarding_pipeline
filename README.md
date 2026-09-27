# Application Onboarding Data Pipeline

## 1. Setup

This project was developed and run on **macOS**, using **Homebrew** to install
**Colima** as the Docker runtime.

### 1.1 Install Homebrew and Colima (macOS)

Install Homebrew (skip if already installed):

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

Install Colima together with the Docker CLI and Docker Compose:

```bash
brew install colima docker docker-compose
```

Let the Docker CLI find the Compose plugin, so `docker compose` works:

```bash
mkdir -p ~/.docker/cli-plugins
ln -sfn "$(brew --prefix)/opt/docker-compose/bin/docker-compose" ~/.docker/cli-plugins/docker-compose
```

Start the Colima VM with enough resources for Airflow:

```bash
colima start --cpu 4 --memory 8 --disk 60
docker compose version   # should print the Compose version
```

### 1.2 Get the repository

```bash
git clone <repository-url>
cd <repository-folder>
```

This repository is a **submission**: it already contains the results of my own
run in `data/warehouse/` (the DuckDB database) and `data/output/` (the Parquet
outputs). **Delete both folders before running the pipeline**, so that
everything is produced from scratch on your machine:

```bash
rm -rf data/warehouse data/output
```

The source CSVs in `data/raw/` stay. The pipeline recreates `data/warehouse/`
and `data/output/` when it runs.

### 1.3 Optional: validate the raw CSVs

`scripts/validate_raw_data.py` is **not part of the pipeline** and is not
needed to run it. It only checks that the raw CSVs in `data/raw/` meet the
conditions of the test (row counts, 90-day window, keys, required fields,
accepted values, event order, application state matching its latest events,
JSON validity, …):

```bash
python3 -m pip install pandas
python3 scripts/validate_raw_data.py --dir data/raw   # exit code 0 = all checks passed
```

## 2. Run the pipeline

```bash
colima start --cpu 4 --memory 8 --disk 60   # if Colima is not running yet

cd <repo>
mkdir -p logs plugins data/warehouse data/output

cat > .env <<'EOF'
AIRFLOW_UID=50000
EOF

docker compose build
docker compose up airflow-init    # first time only: metadata DB + admin user
docker compose up -d              # webserver + scheduler
```

> **Why `AIRFLOW_UID=50000`:** it is the uid of the `airflow` user that already
> exists inside the Airflow image. On macOS/Colima, do not use your own uid
> (`AIRFLOW_UID=$(id -u)`, e.g. 501): that user does not exist in the image and
> `airflow-init` hangs.

Open http://localhost:8080 (user `airflow` / password `airflow`) and trigger
the DAG `onboarding_pipeline`, or run it from the CLI:

```bash
docker compose exec airflow-scheduler airflow dags list-import-errors   # should list no errors
docker compose exec airflow-scheduler airflow dags trigger onboarding_pipeline
```

Results land in `data/output/*.parquet`; the warehouse is `data/warehouse/warehouse.duckdb`.

## 3. Logic

### 3.1 Scenario and goal

The data simulates **opening a bank account**. It comes from three files:

| File | Content |
|---|---|
| `applicants.csv` | one row per applicant: type, country, region, acquisition channel |
| `onboarding_applications.csv` | one row per application attempt: current status and milestone times (started, submitted, completed) |
| `onboarding_events.csv` | one row per event of each application: every step started, completed, failed, retried or resumed, with its timestamp |

The results I wanted:

- **Onboarding funnel by date, channel and application type**
- **Time spent in onboarding**, both for the whole application and at each step

For these results, `onboarding_applications` is the main source, because it
holds the movement of each application (its status and milestone times).
`onboarding_events` comes second: it records the exact time of every step.

### 3.2 Model design (star schema)

From those results, the model needs two fact tables:

- **`fact_application_snapshot`**: one row per application. It keeps the time
  taken (`seconds_to_submit`, `seconds_to_complete`), the status, and the
  number of failures (`failure_event_count`) of each application.
- **`fact_onboarding_events`**: one row per event. It keeps the time of every
  event of each application and the time since the previous event
  (`seconds_since_prev_event`), which gives the time spent at each step.

The dimensions are the master data the facts reference: **`dim_applicant`**,
**`dim_application`**, **`dim_channel`** and **`dim_date`**, arranged as a star
schema around the two facts.

### 3.3 Pipeline

The DAG runs six tasks in order:

1. **`ingest_raw`**: load the three CSVs into the `raw` schema **without
   changing anything** (every column read as text).
2. **`build_staging`**: copy them into the `staging` schema, changing **only
   the data types** (timestamps, integers) and **empty values to NULL**. No
   business logic is applied.
3. **`test_staging`**: test the staged data for uniqueness, required fields,
   accepted values, referential integrity, event order, state transitions,
   timestamp order and JSON validity. It also checks that each application's
   status and milestone times match its latest events. Any failure stops the
   pipeline here.
4. **`build_dimensional_model`**: build the dimension and fact tables in the
   `marts` schema.
5. **`test_marts`**: test the model's assumptions:
   - joins are unique: a LEFT JOIN to a dimension never adds rows (each fact
     has exactly as many rows as its staging source, and each fact is unique
     at its grain);
   - keys are filled (a failed lookup gets `-1`) and unique in every dimension;
   - every application key and date key in the facts exists in its dimension;
   - durations are never negative.
6. **`export_parquet`**: join the facts with the dimensions and write the two
   outputs to `data/output/` as Parquet.

### 3.4 Outputs

- **`onboarding_funnel_by_date_channel`**: for each start date, channel and
  application type, how many applications were started and submitted, and
  their status (approved, rejected, abandoned).

- **`onboarding_duration_and_failure_by_channel`**: for each `channel_code`
  and `application_type`, how many applications there were, how long they
  took (to submit, and to a decision), and whether they failed along the way
  (failed events, sessions, abandonment rate). It shows the performance of
  each application channel.

Time spent at each individual step is not exported as a Parquet table; it can
be read from `fact_onboarding_events` (`seconds_since_prev_event` by
`step_name`).
