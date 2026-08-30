# Project 5 — Governance & Serving Layer
### dbt-core · Great Expectations · OpenLineage/Marquez · Apache Superset · Airflow

> **Curriculum position:** Builds on Project 1 (MinIO + ClickHouse), Project 3 (Spark + Iceberg), and Project 4 (Kafka → ClickHouse).
> All three stacks must be **running** before you start this project.

---

## How This Project Fits Into the Stack

```
Project 1 — MinIO + ClickHouse
  Raw NYC Taxi parquet files live in MinIO's raw-zone bucket.
  ClickHouse is the serving warehouse (port 8123 / 19000).
       ↓
Project 3 — Spark + Iceberg
  PySpark reads raw parquet from MinIO → writes to Iceberg tables (also on MinIO).
  Lives at: E:/Personal/Spark and Iceberg/spark-iceberg-lakehouse/
       ↓
Project 4 — Kafka → ClickHouse
  Streaming data flows into ClickHouse via Kafka.
  Its Docker network (kafkaandclickhouse_default) is needed for Superset.
       ↓
Project 5 — THIS PROJECT
  dbt transforms ClickHouse raw tables → marts.
  Great Expectations validates the marts.
  OpenLineage/Marquez traces lineage across Spark + dbt + Airflow.
  Superset visualises the marts via dashboards.
```

---

## Service Port Reference

| Service | URL | Credentials |
|---|---|---|
| ClickHouse HTTP | `http://localhost:8123` | default / (none) |
| ClickHouse (proxy for WSL2) | `http://localhost:18123` | default / (none) |
| **Marquez API** | `http://localhost:5000` | — |
| **Marquez Admin** | `http://localhost:5001` | — |
| **Marquez Web UI** | `http://localhost:3000` | — |
| **Apache Superset** | `http://localhost:8088` | (See `.env` file) |
| **Apache Airflow** | `http://localhost:8080` | (See `.env` file) |
| **maildev (SMTP catcher)** | `http://localhost:1080` | — |

---

## Project Folder Structure

```
Dbt, Great Exceptions, Lineage and Superset/
│
├── governance-compose.yml          ← All governance Docker services
├── marquez-config/
│   └── marquez.yml                 ← Marquez Postgres backend config
├── clickhouse-config/
│   └── listen_override.xml         ← ClickHouse listen_host override
├── architecture.jpg
├── README.md                       ← This file
├── RETROSPECTIVE.md                ← Phase 5 write-up template
│
├── airflow/
│   └── dags/
│       └── nyc_taxi_governance_dag.py   ← Minimal linear DAG (Phase 3)
│
├── spark_jobs/                     ← Bridge to Project 3 Spark jobs
│   ├── __init__.py
│   ├── run_iceberg_ingest.sh       ← spark-submit wrapper with OL listener
│   └── openlineage_spark_config.py ← get_ol_spark_session() helper
│
├── nyc_taxi_dbt/                   ← dbt project root
│   ├── dbt_project.yml
│   ├── profiles.yml                ← ClickHouse connection (port 18123)
│   └── models/
│       ├── staging/
│       │   ├── _sources.yml
│       │   ├── stg_taxi_trips.sql
│       │   └── stg_taxi_zones.sql
│       ├── intermediate/
│       │   ├── _schema.yml
│       │   └── int_trips_enriched.sql   ← joins + derived fields
│       └── marts/
│           ├── _schema.yml
│           ├── fct_trips.sql
│           ├── dim_zones.sql
│           ├── dim_dates.sql
│           └── agg_daily_revenue.sql
│   └── tests/
│       ├── assert_trip_distance_plausible.sql
│       └── assert_daily_agg_row_counts.sql
│
└── great_expectations/
    └── COMPARISON_NOTES.md         ← Phase 2 write-up template
```

---

## Phase 0 — Environment Setup

### Step 1 — Configure Secure Environment Variables

Before starting any services, you must create a local `.env` file containing your credentials:

```bash
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset"
cp .env.example .env
```
*You can edit the `.env` file to customize your admin passwords. This file is safely gitignored.*

### Step 2 — Verify prerequisite Docker networks exist

Open **WSL2** terminal and run:

```bash
docker network ls | grep -E "minio|kafka"
```

Expected output:
```
minioandclickhouse_warehouse_net
kafkaandclickhouse_default
```

If **either network is missing**, start the relevant project first:

```bash
# Missing minioandclickhouse_warehouse_net → start Project 1
cd "/mnt/e/Personal/MinIo and ClickHouse"
docker compose up -d
sleep 15
docker compose ps    # all services should show "healthy"

# Missing kafkaandclickhouse_default → start Project 4
cd "/mnt/e/Personal/Kafka and ClickHouse"
docker compose up -d
sleep 10
docker compose ps
```

### Step 2 — Verify ClickHouse is responding

```bash
curl "http://localhost:8123/ping"
# Expected: Ok.

curl "http://localhost:8123/?query=SHOW+DATABASES"
# Expected: list including nyc_taxi or nyc_taxi_raw
```

If ClickHouse doesn't respond, check the Project 1 stack:
```bash
cd "/mnt/e/Personal/MinIo and ClickHouse"
docker compose logs --tail=20 clickhouse
```

### Step 3 — Start the governance stack (staged)

Navigate to the project folder:

```bash
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset"
```

**Stage 1: Lineage backend** (Marquez needs to be up before Airflow):

```bash
docker compose -f governance-compose.yml up -d marquez-db marquez marquez-web
```

Wait ~30 seconds, then verify:

```bash
# All three should show "Up" or "healthy"
docker compose -f governance-compose.yml ps marquez-db marquez marquez-web

# API health check — must return {"namespaces":[]}
curl http://localhost:5000/api/v1/namespaces
```

If Marquez is not responding after 60 seconds, check logs:
```bash
docker logs marquez 2>&1 | tail -30
# Common cause: DB not ready yet → wait another 30s and retry curl
```

**Stage 2: SMTP catcher** (start before Superset so it can reach maildev):

```bash
docker compose -f governance-compose.yml up -d maildev
curl http://localhost:1080/healthz || echo "maildev may take a moment"
# Open http://localhost:1080 in browser to confirm empty inbox
```

**Stage 3: BI layer**:

```bash
docker compose -f governance-compose.yml up -d superset
# Takes 60–90 seconds on first run (runs db upgrade + creates admin user)
```

Monitor init progress:
```bash
docker logs -f superset 2>&1 | grep -E "(upgrade|admin|init|Listening|server)"
# Stop following once you see: "[INFO] Listening at: http://0.0.0.0:8088"
```

The correct startup sequence in the logs looks like this (takes ~60–90 seconds):
```
INFO  [alembic.env] Migration scripts completed.    ← db upgrade done
Admin User admin created.                           ← create-admin succeeded
Syncing role definition                             ← superset init running
[INFO] Listening at: http://0.0.0.0:8088           ← server is live ✅
```

> **Expected warning — safe to ignore (Flask-Limiter):**
> ```
> UserWarning: Using the in-memory storage for tracking rate limits...
> ```
> This is harmless for local use. See the note in the Troubleshooting section for details.

> **If you see `Username [admin]:` repeating in the logs — STOP immediately.**
> This means the `command:` block in `governance-compose.yml` has a YAML `>` vs `|` issue.
> The `>` (folded scalar) collapses newlines into spaces, breaking multi-line `create-admin` args.
>
> **Recovery steps:**
> ```bash
> # 1. Stop and remove the stuck container
> docker compose -f governance-compose.yml stop superset
> docker compose -f governance-compose.yml rm -f superset
>
> # 2. Find and remove the Superset data volume (corrupted SQLite state)
> docker volume ls | grep superset
> docker volume rm <volume-name-from-above>
>
> # 3. Check governance-compose.yml — the superset command: block must use | not >
> #    and create-admin must be on ONE line:
> #    command: |
> #      /bin/sh -c "
> #        superset db upgrade &&
> #        superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@admin.com --password admin &&
> #        superset init &&
> #        /usr/bin/run-server.sh"
>
> # 4. Restart cleanly
> docker compose -f governance-compose.yml up -d superset
> ```

Verify: open `http://localhost:8088` → login with **admin / admin**


**Stage 4: Airflow** (requires Marquez healthy):

```bash
docker compose -f governance-compose.yml up -d airflow
```

Wait ~45 seconds, then:
```bash
docker compose -f governance-compose.yml ps airflow
# Should show "Up"
docker logs airflow 2>&1 | tail -20
# Look for: "Initialized the database" and "Starting the web server"
```

Verify: open `http://localhost:8080` → login with **admin** and the auto-generated password.

> **Note on first login:** Airflow standalone mode creates a random admin password automatically on its first run.
> To find your generated password, run this command in your terminal:
> ```bash
> docker exec airflow cat standalone_admin_password.txt
> ```
> *(The username is always `admin`)*

**Stage 5: ClickHouse proxy** (WSL2 → ClickHouse bridge):

```bash
docker compose -f governance-compose.yml up -d clickhouse-proxy
sleep 5

# Verify the proxy is bridging correctly
curl http://localhost:18123/ping
# Expected: Ok.
```

### Step 4 — Confirm all containers are healthy

```bash
docker compose -f governance-compose.yml ps
```

Expected output:

```
NAME               STATUS          PORTS
marquez-db         Up (healthy)    5432/tcp
marquez            Up (healthy)    0.0.0.0:5000->5000/tcp, 0.0.0.0:5001->5001/tcp
marquez-web        Up              0.0.0.0:3000->3000/tcp
superset           Up (healthy)    0.0.0.0:8088->8088/tcp
airflow            Up              0.0.0.0:8080->8080/tcp
maildev            Up              0.0.0.0:1080->1080/tcp, 0.0.0.0:1025->1025/tcp
clickhouse-proxy   Up              0.0.0.0:18123->18123/tcp
```

### Step 5 — Set up Python virtual environment (WSL2 — NOT inside Docker)

dbt and Great Expectations run natively in WSL2, not inside any container.

```bash
# Create venv
python3 -m venv ~/venvs/governance
source ~/venvs/governance/bin/activate

# Install all packages needed across all phases
pip install \
  dbt-core \
  dbt-clickhouse \
  great_expectations \
  openlineage-dbt==1.29.0 \
  openlineage-python==1.29.0 \
  clickhouse-sqlalchemy \
  clickhouse-connect \
  apache-airflow-providers-openlineage

# Verify key installs
dbt --version
great_expectations --version
pip show openlineage-dbt | grep Version
```

Expected output (versions confirmed working):
```
Core:
  - installed: 1.12.3        ← dbt-core
  - latest:    1.12.3 - Up to date!

Plugins:
  - clickhouse: 1.12.x - Up to date!   ← must match dbt-core minor version

great_expectations, version 0.18.22
Version: 1.29.0                         ← openlineage-dbt
```

> **If you see `clickhouse: 1.9.3 - Update available!`**
> You can **safely ignore this warning**. The `dbt-clickhouse` plugin version `1.9.3`
> is fully compatible with `dbt-core 1.12.3` for this project. Do NOT run
> `pip install --upgrade dbt-clickhouse` as it may downgrade `dbt-core` and introduce
> dependency conflicts with Python 3.14 (specifically a `mashumaro` serialization bug).


### Step 6 — Verify dbt can connect to ClickHouse

```bash
source ~/venvs/governance/bin/activate
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset/nyc_taxi_dbt"

dbt debug
```

Expected output:
```
Connection:
  host: localhost
  port: 18123
  schema: nyc_taxi_marts
  ...
Connection test: OK connection ok
All checks passed!
```

If connection fails:
```bash
# 1. Is the proxy running?
curl http://localhost:18123/ping

# 2. Is ClickHouse password correct?
curl "http://localhost:8123/?query=SELECT+1"

# 3. Does the source database exist?
curl "http://localhost:8123/?query=SHOW+DATABASES"
# Must include the database referenced in _sources.yml (nyc_taxi_raw)
```

### ✅ Phase 0 Checklist

- [ ] `docker network ls` shows both `minioandclickhouse_warehouse_net` and `kafkaandclickhouse_default`
- [ ] `curl http://localhost:5000/api/v1/namespaces` → `{"namespaces":[]}`
- [ ] `curl http://localhost:18123/ping` → `Ok.`
- [ ] `http://localhost:8088` loads Superset (admin / admin)
- [ ] `http://localhost:8080` loads Airflow (airflow / airflow)
- [ ] `http://localhost:1080` loads maildev inbox (empty)
- [ ] `dbt debug` → `All checks passed!`

---

## Phase 1 — dbt Project: Build the Model Layers

### Understanding the model DAG

```
Source (ClickHouse: nyc_taxi_iceberg)
    stg_taxi_trips  ──┐
                      ├──→ int_trips_enriched ──→ fct_trips        (MergeTree)
    stg_taxi_zones  ──┘        (View)           ├──→ dim_zones      (MergeTree)
                                                 ├──→ dim_dates      (MergeTree)
                                                 └──→ agg_daily_revenue (SummingMergeTree)
```

Each layer has a specific role:
- **Staging** — 1:1 with raw source, renaming + casting only, no joins
- **Intermediate** — joins + derived fields (trip duration, speed, tip%), materialised as View
- **Marts** — physical ClickHouse tables with engine configs, used by Superset

### Step 1 — Check your source tables exist

Before running dbt, verify the raw source tables are present in ClickHouse:

```bash
# List all tables in the Iceberg database
curl "http://localhost:8123/?query=SHOW+TABLES+FROM+nyc_taxi_iceberg"
```

Expected tables: `yellow_trips`

If these tables are missing, the NYC taxi data hasn't been loaded yet. Load it from Project 1:
```bash
cd "/mnt/e/Personal/MinIo and ClickHouse"
# Follow the data loading steps in that project's README
```

### Step 2 — Run all models

```bash
source ~/venvs/governance/bin/activate
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset/nyc_taxi_dbt"

# Run all models (staging → intermediate → marts) in dependency order
dbt run
```

Watch the output — you should see each model run in order:
```
1 of 8 START seed file nyc_taxi_marts.taxi_zones ....................... [RUN]
2 of 8 START sql view model nyc_taxi_marts.stg_taxi_trips .............. [RUN]
3 of 8 START sql view model nyc_taxi_marts.stg_taxi_zones .............. [RUN]
4 of 8 START sql view model nyc_taxi_marts.int_trips_enriched .......... [RUN]
5 of 8 START sql table model nyc_taxi_marts.fct_trips .................. [RUN]
6 of 8 START sql table model nyc_taxi_marts.dim_zones .................. [RUN]
7 of 8 START sql table model nyc_taxi_marts.dim_dates .................. [RUN]
8 of 8 START sql table model nyc_taxi_marts.agg_daily_revenue .......... [RUN]
Finished running 7 models in X.XXs
```

If a model fails:
```bash
# See detailed error for a specific model
dbt run --select fct_trips

# Check what ClickHouse says about your SQL
dbt compile --select fct_trips
# Opens target/compiled/.../fct_trips.sql — paste this into clickhouse-client to debug
```

### Step 3 — Run all tests

```bash
dbt test
```

This runs:
- **Generic tests** from `_schema.yml` files: `not_null`, `unique`, `relationships`, `accepted_values`
- **Singular tests** from `tests/` directory:
  - `assert_trip_distance_plausible.sql` — catches trips with impossible physics (speed > 100 mph, negative duration)
  - `assert_daily_agg_row_counts.sql` — catches silent row loss between `fct_trips` and `agg_daily_revenue`

Expected output:
```
Finished running 15 tests in X.XXs
Passed: 15
Failed: 0
```

### Step 4 — Run build (run + test in one pass)

```bash
dbt build
```

This is the production command — runs models then tests, in dependency order.

### Step 5 — Verify marts in ClickHouse

```bash
# List all tables in the marts schema
curl "http://localhost:8123/?query=SHOW+TABLES+FROM+nyc_taxi_marts"

# Quick row count check
curl "http://localhost:8123/?query=SELECT+count()+FROM+nyc_taxi_marts.fct_trips"
```

### Step 6 — Browse the dbt docs DAG

```bash
dbt docs generate
dbt docs serve --port 8081
```

Open `http://localhost:8081` → click **Project** tab → explore the lineage DAG. You should see the 3-layer structure with all 7 models connected.

> Press `Ctrl+C` to stop the docs server when done.

### ✅ Phase 1 Checklist

- [ ] `dbt run` completes with 0 errors across all 7 models
- [ ] `dbt test` passes all 15+ tests (0 failures)
- [ ] `SHOW TABLES FROM nyc_taxi_marts` shows: `fct_trips`, `dim_zones`, `dim_dates`, `agg_daily_revenue`
- [ ] `fct_trips` row count > 0
- [ ] dbt docs DAG shows all 3 layers connected at `http://localhost:8081`

---

## Phase 2 — Great Expectations

Great Expectations (GE) is a second validation layer that runs *on top of* the ClickHouse marts — independent of dbt. The goal is to use expectations that dbt's boolean SQL tests can't express: distributional checks, cross-column comparisons, and tolerance-range row counts.

### Step 1 — Run the Automated Validation Suite

To circumvent CLI limitations with this version of Great Expectations, we have completely automated the suite initialization and validation process using the `run_ge.py` Python script. 

This script will seamlessly:
- Initialize the Great Expectations context
- Connect to the `clickhouse_marts` datasource via SQLAlchemy
- Define and save the `trips_physics_suite` (distributional checks, cross-column comparisons)
- Run a checkpoint against the ClickHouse `fct_trips` mart
- Build and open the Data Docs

```bash
# Ensure you're in the governance virtual environment
source ~/venvs/governance/bin/activate
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset"

# Execute the validation pipeline
python3 scripts/run_ge.py
```

*Note: The checkpoint will evaluate to `Success: False` because the real-world taxi data contains out-of-range trip distances (e.g. 159,000+ miles), which is exactly what Great Expectations is designed to catch!*

### Step 2 — Review Data Docs

The Python script will automatically compile your Data Docs . 

You can find the generated HTML site at:
```
gx/uncommitted/data_docs/local_site/index.html
```

Open this `index.html` file in your browser to explore the specific expectation failures side-by-side with your dbt documentation.

### ✅ Phase 2 Checklist

- [ ] `run_ge.py` executed successfully
- [ ] `Success: False` received (due to expected NYC taxi real-world physics anomalies)
- [ ] Data Docs generated and reviewed in browser

### Step 7 — Phase 3: OpenLineage & Airflow
Execute the full end-to-end pipeline (Spark → Iceberg → ClickHouse) to generate lineage in Marquez.
```bash
# 1. Start the Airflow scheduler in the background
docker compose -f governance-compose.yml up -d airflow

# 2. Trigger the DAG manually via Airflow UI (http://localhost:8080)
# Go to the UI and unpause the `nyc_taxi_governance_dag` DAG, then trigger it.

# 3. Open Marquez UI to view lineage (http://localhost:3000)
# You should see the Spark job, dbt models, and ClickHouse datasets connected.
```

### Step 8 — Phase 4: Superset Dashboards
Connect Superset to the newly built ClickHouse marts and create visualizations.
```bash
# Ensure Superset is running and has the ClickHouse drivers injected into its active path
docker exec -u root superset pip install -t /app/pythonpath clickhouse-sqlalchemy==0.2.4 clickhouse-connect
docker compose -f governance-compose.yml restart superset

# 1. Login to Superset: http://localhost:8088 (admin/admin)
# 2. Add Database -> ClickHouse (SQLAlchemy URI: clickhousedb://default@clickhouse:8123/nyc_taxi_marts)
# 3. Add Datasets: `fct_trips`, `agg_daily_revenue`
# 4. Build Revenue & Operations dashboard
```

---

## Phase 3 — OpenLineage & Marquez Wiring (Cross-Tool Lineage)

This phase wires three tools — Spark, dbt, and Airflow — into a single lineage graph in Marquez. The payoff is being able to trace a data quality failure backward from a failing dbt test all the way to the raw source parquet file in MinIO.

### Step 1 — Wire dbt → Marquez

```bash
source ~/venvs/governance/bin/activate
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset/nyc_taxi_dbt"

# Set OpenLineage env vars
export OPENLINEAGE_URL=http://localhost:5000
export OPENLINEAGE_NAMESPACE=nyc_taxi_pipeline

# Ensure OpenLineage packages are up-to-date (older versions like 1.29.0 lack ClickHouse support)
pip install --upgrade openlineage-dbt openlineage-python openlineage-sql openlineage-integration-common

# Run dbt via the openlineage wrapper (emits START/COMPLETE events per model)
dbt-ol run
```

Wait for it to complete, then verify events landed in Marquez:

```bash
curl "http://localhost:5000/api/v1/namespaces/nyc_taxi_pipeline/jobs"
  | python3 -m json.tool | head -60
```

Expected: JSON containing job names like `nyc_taxi_dbt.fct_trips`, `nyc_taxi_dbt.dim_zones` etc.

Also check the Marquez Web UI at `http://localhost:3000`:
- Click **Namespaces** → select `nyc_taxi_pipeline`
- You should see dbt model nodes appearing as jobs

### Step 2 — Wire Spark → Marquez

```bash
# Run the Spark bridge script inside the Airflow container (which has pyspark installed)
docker exec airflow bash /opt/spark_jobs/run_iceberg_ingest.sh
```

This runs the Project 3 `02_ingest_to_iceberg.py` job with the OpenLineage Spark listener JAR attached. Marquez receives events for the raw parquet → Iceberg write.

Verify Spark job appeared in Marquez:
```bash
curl "http://localhost:5000/api/v1/namespaces/nyc_taxi_pipeline/jobs"
  | python3 -m json.tool | grep "name"
# Should now show BOTH dbt models AND the Spark ingestion job
```

In Marquez UI, you should now see a connected graph: `[MinIO parquet] → [Spark job] → [Iceberg table] → [dbt models] → [ClickHouse marts]`

### Step 3 — Install OpenLineage provider in Airflow

```bash
docker exec airflow python3 -m pip install apache-airflow-providers-openlineage
```

Force Airflow to pick up the new DAG file:
```bash
docker exec -it airflow airflow dags reserialize
```

Verify the DAG is registered:
```bash
docker exec -it airflow airflow dags list | grep nyc_taxi
# Expected: nyc_taxi_governance
```

### Step 4 — Trigger the Airflow DAG

1. Open `http://localhost:8080`
2. Login: **airflow / airflow**
3. Find the `nyc_taxi_governance` DAG in the list
4. Click the **toggle** on the left to unpause it (turns blue)
5. Click the **▶ Run** button → Trigger DAG
6. Click on the DAG name → watch the task boxes turn green

Monitor task logs:
- Click `spark_raw_to_iceberg` task → **Logs**
- Click `dbt_build_marts` task → **Logs** — should show dbt build output

### Step 5 — Bad-row incident trace exercise

This is the core learning exercise of Phase 3. You inject a bad row upstream, run the pipeline, and use Marquez to trace it.

**Inject a bad row:**
```bash
# Connect to ClickHouse and insert a row with an impossible fare
curl "http://localhost:8123/"
  --data "INSERT INTO nyc_taxi_raw.yellow_trips
  (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime,
   passenger_count, trip_distance, PULocationID, DOLocationID,
   payment_type, fare_amount, tip_amount, total_amount)
  VALUES (1, '2023-01-15 10:00:00', '2023-01-15 09:00:00',
   1, 5.0, 132, 236, 1, -99999, 0, -99999)"
```

> This row has `dropoff < pickup` and a negative fare — the singular test will catch it.

**Re-run the full DAG:**
```bash
# In Airflow UI: click ▶ Run again on nyc_taxi_governance
# OR trigger via CLI:
docker exec -it airflow airflow dags trigger nyc_taxi_governance
```

**Watch it fail in Airflow:**
- The `dbt_build_marts` task should turn red
- Click it → Logs → look for the test failure message

**Trace it in Marquez:**
1. Open `http://localhost:3000`
2. Click **Namespaces** → `nyc_taxi_pipeline`
3. Click the failing job (e.g., `nyc_taxi_dbt.assert_trip_distance_plausible`)
4. Click on the **input dataset** → `fct_trips`
5. Click on its **upstream** → `int_trips_enriched` → `stg_taxi_trips`
6. Follow upstream → you reach the `iceberg-ingest-governance` Spark job
7. That job's input dataset points to the MinIO source parquet

You've now traced a dbt test failure back to the raw source using column-level lineage.

**Fix the bad row and re-run:**
```bash
curl "http://localhost:8123/"
  --data "ALTER TABLE nyc_taxi_raw.yellow_trips DELETE
  WHERE fare_amount = -99999"

# Re-run the DAG from Airflow UI
```

### ✅ Phase 3 Checklist

- [ ] `dbt-ol run` emits events — Marquez shows dbt model job nodes
- [ ] Spark job appears in Marquez after running `run_iceberg_ingest.sh`
- [ ] `airflow dags list` shows `nyc_taxi_governance`
- [ ] Airflow DAG runs end-to-end without errors (green tasks)
- [ ] Bad-row incident: failure visible in Airflow logs, traced in Marquez UI from `fct_trips` upstream to MinIO source

---

## Phase 4 — Superset Dashboards

### Step 1 — Install ClickHouse driver in Superset

```bash
docker exec -u root superset pip install -t /app/pythonpath clickhouse-sqlalchemy==0.2.4 clickhouse-connect
docker compose -f governance-compose.yml restart superset

# Wait 30s for Superset to restart, then verify it's back up
curl -s http://localhost:8088/health | python3 -m json.tool
```

### Step 2 — Connect Superset to ClickHouse

1. Open `http://localhost:8088` → login: **admin / admin**
2. Top menu: **Settings** → **Database Connections** → **+ Database**
3. Choose: **ClickHouse** (or type "ClickHouse" in the search if not listed)
4. Enter the SQLAlchemy URI:
   ```
   clickhousedb://default@clickhouse:8123/nyc_taxi_marts
   ```
   > Use the container name `clickhouse` (not `localhost`) — Superset runs inside Docker and can reach ClickHouse by container name via the shared `existing-data-net` network.

5. Click **Test Connection** → should show "Connection looks good!"
6. Click **Connect**

### Step 3 — Pre-create ClickHouse objects for Dashboards 4 & 5

Run these SQL commands in ClickHouse:

```bash
# Option A: via curl
curl "http://localhost:8123/" --data "
CREATE MATERIALIZED VIEW IF NOT EXISTS nyc_taxi_marts.mv_daily_revenue
ENGINE = SummingMergeTree()
ORDER BY pickup_date
AS SELECT
    toDate(pickup_datetime) AS pickup_date,
    count()                 AS trip_count,
    sum(fare_amount)        AS total_fare,
    sum(tip_amount)         AS total_tips,
    sum(total_amount)       AS total_revenue
FROM nyc_taxi_marts.fct_trips
GROUP BY pickup_date;
"

curl "http://localhost:8123/" --data "
CREATE TABLE IF NOT EXISTS nyc_taxi_marts.dbt_test_results (
    run_at     DateTime,
    model_name String,
    test_name  String,
    status     Enum8('pass' = 1, 'fail' = 0),
    failures   UInt32
) ENGINE = MergeTree()
ORDER BY run_at;
"
```

Or use Superset's **SQL Lab** (step 4b) to run them.

### Step 4a — SQL Lab exploration

Before building dashboards, spend time in SQL Lab:
1. Top menu: **SQL** → **SQL Lab**
2. Left panel: select **Database** → `ClickHouse`, **Schema** → `nyc_taxi_marts`
3. Run exploratory queries:

```sql
-- Explore the fact table
SELECT * FROM nyc_taxi_marts.fct_trips LIMIT 10;

-- Daily trip counts
SELECT pickup_date, count() AS trips, round(sum(total_amount), 2) AS revenue
FROM nyc_taxi_marts.fct_trips
GROUP BY pickup_date
ORDER BY pickup_date DESC
LIMIT 30;

-- Borough-level summary
SELECT pickup_borough, count() AS trips, round(avg(fare_amount), 2) AS avg_fare
FROM nyc_taxi_marts.fct_trips
GROUP BY pickup_borough
ORDER BY trips DESC;

-- Dashboard 4: MV vs raw GROUP BY timing
-- Run these separately and compare the query duration shown below the editor
SELECT pickup_date, trip_count, total_revenue
FROM nyc_taxi_marts.mv_daily_revenue
ORDER BY pickup_date;

SELECT toDate(pickup_datetime) AS pickup_date, count() AS trip_count, sum(total_amount) AS total_revenue
FROM nyc_taxi_marts.fct_trips
GROUP BY pickup_date
ORDER BY pickup_date;
```

Save interesting queries using **Save** button — they persist for later reference.

### Step 4b — Build Dashboard 1: Trip Volume & Revenue Trends

1. **Charts** → **+ Chart**
2. Dataset: `agg_daily_revenue`
3. Chart type: **Line Chart**
4. X-axis: `pickup_date`
5. Metrics: `SUM(total_revenue)`, `SUM(trip_count)`
6. Save as "Daily Revenue Trend"
7. **Dashboards** → **+ Dashboard** → name it "Trip Volume & Revenue"
8. Drag the chart in → Save

### Step 4c — Build Dashboard 2: Zone Heatmap

1. **Charts** → **+ Chart**
2. Dataset: `fct_trips`
3. Chart type: **Heatmap**
4. X-axis (Dimension): `pickup_borough`
5. Y-axis (Dimension): `dropoff_borough`
6. Metric: `COUNT(*)` or `SUM(total_amount)`
7. Save as "Pickup vs Dropoff Heatmap"

### Step 4d — Build Dashboard 3: Fare & Tip Distribution

1. **Charts** → **+ Chart**
2. Dataset: `fct_trips`
3. Chart type: **Histogram** (better for raw distributions in Superset)
4. Column: `fare_amount` (or `tip_amount`)
5. Dimensions: `payment_type`
6. Save as "Fare Distribution by Payment Type"

### Step 4e — Build Dashboard 4: MV Speed Comparison

**1. Chart A (Materialized View Speed)**
1. **Datasets** → **+ Dataset**: Add the `mv_daily_revenue` table.
2. **Charts** → **+ Chart** → Dataset: `mv_daily_revenue`, Chart Type: **Line Chart**.
3. X-axis: `pickup_date`, Metrics: `SUM(total_revenue)`.
4. Save as "Chart A - MV Speed".

**2. Chart B (Raw Query Speed)**
1. **SQL Lab** → **SQL Editor** → Select `ClickHouse` database.
2. Paste and run this exact custom SQL:
   ```sql
   SELECT toDate(pickup_datetime) AS pickup_date,
          sum(total_amount) AS total_revenue
   FROM fct_trips GROUP BY pickup_date ORDER BY pickup_date
   ```
3. Click the **Create Chart** (or **Explore**) button located below the SQL editor window.
4. Chart type: **Line Chart**, X-axis: `pickup_date`, Metrics: `SUM(total_revenue)`.
5. Save as "Chart B - Raw Query Speed".

**3. Compare them**
1. **Dashboards** → **+ Dashboard** → name it "MV vs Raw Query Speed".
2. Place both charts side-by-side and notice how much faster the Materialized View loads compared to the raw query!
3. Record the exact query duration difference in `RETROSPECTIVE.md`.

### Step 4f — Build Dashboard 5: Ops / Governance

1. **Charts** → **+ Chart**
2. Dataset: `dbt_test_results` (will be empty until you populate it — see note below)
3. Chart type: **Table**
4. Columns: `run_at`, `model_name`, `test_name`, `status`, `failures`
5. Save as "dbt Test Results"

**Populate test results:** After each `dbt test` run, you can manually insert results:
```bash
curl "http://localhost:8123/" --data "
INSERT INTO nyc_taxi_marts.dbt_test_results VALUES
(now(), 'fct_trips', 'not_null_pickup_datetime', 'pass', 0),
(now(), 'dim_zones', 'unique_zone_id', 'pass', 0)
"
```

### Step 5 — Configure alerts and reports

**A. Set up an Alert (Data Threshold)**
1. On the top menu, click **Alerts & Reports** (in some versions, it is located under the **Settings** dropdown menu).
2. Make sure you are on the **Alerts** tab, and click the **+ Alert** button.
3. **Alert Name**: `Low Daily Trip Count`
4. Expand the **Query** section:
   - **Database**: Select your `ClickHouse` connection.
   - **SQL Query**: Paste the following:
     ```sql
     SELECT count() FROM nyc_taxi_marts.agg_daily_revenue
     WHERE pickup_date = today()
     ```
5. Expand the **Trigger** (or **Alert condition**) section:
   - **Condition**: `< (Smaller than)`
   - **Threshold**: `100` (This means if the query returns a number smaller than 100, it triggers the alert).
6. Expand the **Alert contents** section:
   - **Content type**: `Chart` (Do NOT choose Dashboard, as this minimal Docker setup doesn't have a headless browser installed for screenshots!)
   - **Select chart**: Choose any chart you made.
   - **Content format**: `Text` or `CSV`
7. Expand the **Notification Method** section:
   - **Method**: `Email`
   - **Recipients**: Enter any dummy email address (e.g., `admin@test.com`).
8. Click **Save** (or **Add**).

**B. Set up a Scheduled Report (Dashboard Snapshot)**
1. Go back to **Alerts & Reports** and click on the **Reports** tab at the top.
2. Click the **+ Report** button.
3. **Report Name**: `Daily Revenue Dashboard Snapshot`
4. **Dashboard**: Click the dropdown and search for your "Trip Volume & Revenue" dashboard.
5. Expand the **Schedule** section:
   - Type in the cron schedule: `0 8 * * *` (This sends the report daily at 08:00 AM).
6. Expand the **Notification Method** section:
   - **Method**: `Email`
   - **Recipients**: `admin@test.com`
7. Click **Save** (or **Add**).

**Verify emails in maildev:**
1. Open a new browser tab to `http://localhost:1080` (this is your local Maildev inbox).
2. Back in Superset, go to the **Alerts & Reports** list page.
3. Find your `Low Daily Trip Count` alert in the list, and under the **Actions** column on the far right, click the **Play icon (▶️)** to force it to run immediately.
4. Go back to your Maildev tab. The alert email with the dashboard screenshot attached should appear within a few seconds!

### ✅ Phase 4 Checklist

- [ ] ClickHouse connection test passes in Superset
- [ ] `mv_daily_revenue` materialized view created in ClickHouse
- [ ] `dbt_test_results` table created in ClickHouse
- [ ] All 5 dashboards created and rendering with real data
- [ ] Dashboard 4: MV vs raw query timing numbers recorded
- [ ] Alert fires and appears in maildev (`http://localhost:1080`)
- [ ] Scheduled report saved

---

## Phase 5 — Synthesis & Wrap-up

### Step 1 — Write the retrospective

Edit `RETROSPECTIVE.md` in the project root. The template has structured prompts for:
1. Your dbt vs GE conclusion (not the textbook answer — what you actually found)
2. What Marquez showed you that log-tailing wouldn't have
3. Dashboard 4 timing numbers (MV vs raw GROUP BY)
4. One surprise from each phase

### Step 2 — Full teardown and clean restart test

The final verification: prove the entire stack comes up reproducibly from zero.

```bash
# Full teardown
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset"
docker compose -f governance-compose.yml down

# Verify containers are gone
docker compose -f governance-compose.yml ps
# Should show empty

# Clean restart — staged
docker compose -f governance-compose.yml up -d marquez-db marquez marquez-web
sleep 40
curl http://localhost:5000/api/v1/namespaces   # must respond before proceeding

docker compose -f governance-compose.yml up -d maildev superset
sleep 90   # Superset takes time on first init after volume restart

docker compose -f governance-compose.yml up -d airflow clickhouse-proxy
sleep 30

# Verify all up
docker compose -f governance-compose.yml ps

# Re-run the dbt pipeline
source ~/venvs/governance/bin/activate
cd nyc_taxi_dbt
dbt build
```

Record any instability in `RETROSPECTIVE.md`.

---

## Troubleshooting

### Marquez not starting
```bash
docker logs marquez 2>&1 | tail -40
```
Common causes:
- **DB not ready** — marquez-db needs ~10s after `Up` before accepting connections. Marquez has a `depends_on: healthy` check; if it keeps restarting, increase the `start_period` in the healthcheck.
- **Config not found** — verify `./marquez-config/marquez.yml` is mounted. Check: `docker inspect marquez | grep -A5 Mounts`
- **Port conflict** — something else using port 5000: `ss -tlnp | grep 5000`

### dbt debug fails — connection refused
```bash
# Is the proxy running?
docker compose -f governance-compose.yml ps clickhouse-proxy

# Is the proxy bridging correctly?
curl http://localhost:18123/ping   # must return "Ok."

# Is ClickHouse actually listening?
curl http://localhost:8123/ping    # from WSL2 directly
```

### OpenLineage events not appearing in Marquez
```bash
# Check raw event log (bypasses UI rendering)
curl "http://localhost:5000/api/v1/events?limit=5" | python3 -m json.tool

# Common cause: version mismatch between OL packages
pip show openlineage-dbt openlineage-python | grep Version
# Both must show 1.29.0

# Also check Marquez logs for rejected events
docker logs marquez 2>&1 | grep -i "error\|warn" | tail -20
```

### Airflow DAG not visible
```bash
docker exec -it airflow airflow dags list
# If nyc_taxi_governance is missing:
docker exec -it airflow airflow dags reserialize
docker logs airflow 2>&1 | grep -i "error" | tail -20
# Check DAG file for syntax errors:
python3 -c "import ast; ast.parse(open('/path/to/dag.py').read()); print('OK')"
```

### Superset can't reach ClickHouse
```bash
# Test from inside the Superset container
docker exec -it superset curl http://clickhouse:8123/ping
# If this fails, the containers are not on the same network

# Verify both are on existing-data-net
docker inspect superset | grep -A3 '"minioandclickhouse'
docker inspect clickhouse | grep -A3 '"minioandclickhouse'
```

### Full teardown (WARNING: destroys all data)
```bash
# Destroys all Marquez lineage, Superset dashboards, Airflow state
docker compose -f governance-compose.yml down -v
```

---

## Quick Reference Commands

```bash
# Navigate to project
cd "/mnt/e/Personal/Dbt, Great Exceptions, Lineage and Superset"

# Activate venv (always needed for dbt/GE)
source ~/venvs/governance/bin/activate

# Start stack
docker compose -f governance-compose.yml up -d

# Stop stack (keeps volumes)
docker compose -f governance-compose.yml down

# dbt: full build + test
cd nyc_taxi_dbt && dbt build

# dbt: build with lineage emission
export OPENLINEAGE_URL=http://localhost:5000
export OPENLINEAGE_NAMESPACE=nyc_taxi_pipeline
dbt-ol run

# GE: run both checkpoints
great_expectations checkpoint run fct_trips_checkpoint
great_expectations checkpoint run dim_zones_checkpoint

# Trigger Airflow DAG from CLI
docker exec -it airflow airflow dags trigger nyc_taxi_governance

# Check Marquez jobs
curl http://localhost:5000/api/v1/namespaces/nyc_taxi_pipeline/jobs | python3 -m json.tool
```
