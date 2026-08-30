# Project 5 — Governance & Serving Deep Dive
### dbt-core + Great Expectations + OpenLineage/Marquez + Superset

**Stack context:** builds on your existing WSL2/Docker setup — ClickHouse + MinIO with NYC taxi data already loaded ([[nyc-taxi-clickhouse-project]]), and the Spark+Iceberg lakehouse ([[spark-iceberg-lakehouse-project]]). This project sits on top of ClickHouse as the serving/warehouse layer and adds the governance layer your learning-order roadmap (Spark → Iceberg → dbt → Kafka → Airflow) has been building toward.

**Note on Airflow:** you've completed the MinIO+ClickHouse, Spark+Iceberg, and Kafka→ClickHouse projects but haven't done the dedicated Airflow deep dive yet. This plan uses Airflow here only as scaffolding for OpenLineage — a minimal, linear DAG with no sensors, branching, backfills, or XComs. Those are the actual content of your Airflow project and are deliberately left for that dedicated deep dive later. What you build here just needs to exist so lineage events have a DAG to attach to; when you circle back to the real Airflow project, the environment is already running and you go straight into the harder material instead of spending a day on setup.

---

## Phase 0 — Environment scaffolding (Day 1)

Add these services to your existing `docker-compose.yml` (or a new `governance-compose.yml` you run alongside it):

```yaml
services:
  marquez-db:
    image: postgres:15
    environment:
      POSTGRES_USER: marquez
      POSTGRES_PASSWORD: marquez
      POSTGRES_DB: marquez
    volumes:
      - marquez-db-data:/var/lib/postgresql/data

  marquez:
    image: marquezproject/marquez:latest
    ports:
      - "5000:5000"   # API
      - "5001:5001"   # Admin
    depends_on:
      - marquez-db
    environment:
      MARQUEZ_PORT: 5000
    command: ["--config", "/config/marquez.yml"]

  marquez-web:
    image: marquezproject/marquez-web:latest
    ports:
      - "3000:3000"
    environment:
      MARQUEZ_HOST: marquez
      MARQUEZ_PORT: 5000
    depends_on:
      - marquez

  superset:
    image: apache/superset:latest
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: "change-me-local-only"
    volumes:
      - superset-data:/app/superset_home
    command: >
      /bin/sh -c "
      superset db upgrade &&
      superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@admin.com --password admin &&
      superset init &&
      /usr/bin/run-server.sh"

  # Only if you don't already have Airflow running
  airflow:
    image: apache/airflow:2.10.2-python3.11
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      OPENLINEAGE_URL: http://marquez:5000
      OPENLINEAGE_NAMESPACE: nyc_taxi_pipeline
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./dbt:/opt/dbt
    command: standalone

volumes:
  marquez-db-data:
  superset-data:
```

Bring it up in stages, verifying each before moving on:
```bash
docker compose up -d marquez-db marquez marquez-web
# check http://localhost:3000 loads (empty Marquez UI)
docker compose up -d superset
# check http://localhost:8088 loads, log in as admin/admin
docker compose up -d airflow   # only if needed
```

**Local Python env (outside Docker, since dbt/GE run better native in WSL2):**
```bash
python3 -m venv ~/venvs/governance
source ~/venvs/governance/bin/activate
pip install dbt-core dbt-clickhouse great_expectations openlineage-dbt openlineage-python
```

---

## Phase 1 — dbt project against ClickHouse (Days 2–4)

**1a. Init and connection**
```bash
dbt init nyc_taxi_dbt --adapter clickhouse
```
`profiles.yml`:
```yaml
nyc_taxi_dbt:
  target: dev
  outputs:
    dev:
      type: clickhouse
      schema: nyc_taxi_marts
      host: localhost
      port: 8123
      user: default
      password: ""
      secure: false
```

**1b. Layered model structure** — mirror the classic dbt layering on top of your existing raw taxi tables in ClickHouse:

- `models/staging/stg_taxi_trips.sql` — 1:1 with source, light renaming/casting only, one `stg_` model per raw table you loaded via MinIO
- `models/staging/stg_taxi_zones.sql` — taxi zone lookup
- `models/intermediate/int_trips_enriched.sql` — joins trips to zones, adds derived fields (trip duration, speed, tip %)
- `models/marts/fct_trips.sql` — grain-per-trip fact table, ClickHouse `MergeTree` engine, partitioned by pickup date
- `models/marts/dim_zones.sql`, `models/marts/dim_dates.sql` — conformed dimensions
- `models/marts/agg_daily_revenue.sql` — pre-aggregated mart for Superset (this is the one you'll later compare against a ClickHouse materialized view)

Use `dbt-clickhouse` engine configs in each mart's config block (`engine='MergeTree()'`, `order_by`, `partition_by`) — this is where dbt-on-ClickHouse differs meaningfully from dbt-on-Postgres/Snowflake, worth exploring deliberately since it's new territory for you.

**1c. dbt tests** — add both generic (schema.yml) and singular tests:
- Generic: `not_null`, `unique`, `relationships` (fct_trips.zone_id → dim_zones), `accepted_values` for categorical columns
- Singular: a custom test asserting trip distance/duration ratios are physically plausible (catches bad rows), a custom test asserting daily aggregate row counts match source counts (catches dropped rows)
- Run `dbt test` and `dbt build` and inspect the pass/fail output — note dbt tests are boolean, SQL-query-based, and run inside the warehouse

**Checkpoint:** `dbt docs generate && dbt docs serve` — browse the auto-generated DAG and docs site before moving on.

---

## Phase 2 — Great Expectations as a second validation layer (Days 5–6)

```bash
great_expectations init
great_expectations datasource new   # point at ClickHouse via SQLAlchemy (clickhouse-sqlalchemy driver)
```

- Build one Expectation Suite per mart (`fct_trips_suite`, `dim_zones_suite`)
- Deliberately write expectations dbt tests *can't* express cleanly, to make the comparison real rather than duplicative:
  - `expect_column_values_to_be_between` with dynamically computed bounds (e.g. based on rolling statistics, not a fixed literal)
  - `expect_column_pair_values_A_to_be_greater_than_B` (dropoff_time > pickup_time)
  - `expect_column_quantile_values_to_be_between` on trip fare distribution
  - `expect_table_row_count_to_be_between` with a tolerance range rather than an exact match
  - A multi-column distributional expectation (e.g. `expect_compound_columns_to_be_unique` or a custom expectation)
- Generate a **Data Docs** site (`great_expectations docs build`) and compare it side-by-side with the dbt docs site
- Write up (a short README or notes file, not code) the actual difference you find: where dbt tests were sufficient, where GE's expressiveness earned its extra complexity, and where GE felt like overkill

This comparison write-up is the actual deliverable of this phase — the point isn't "implement both," it's forming your own opinion on when each tool earns its place.

---

## Phase 3 — OpenLineage → Marquez wiring (Days 7–9)

This is the most fiddly phase; budget extra time and expect to debug event emission.

**3a. dbt → OpenLineage**
```bash
pip install openlineage-dbt
export OPENLINEAGE_URL=http://localhost:5000
export OPENLINEAGE_NAMESPACE=nyc_taxi_pipeline
dbt-ol run   # wraps `dbt run`, emits START/COMPLETE lineage events
```
Verify events land in Marquez by checking `http://localhost:5000/api/v1/namespaces/nyc_taxi_pipeline/jobs`.

**3b. Spark → OpenLineage**
Add the OpenLineage Spark listener JAR to your existing Spark session config from the Iceberg project:
```python
spark = (SparkSession.builder
    .config("spark.jars.packages", "io.openlineage:openlineage-spark_2.12:1.x.x")
    .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
    .config("spark.openlineage.transport.type", "http")
    .config("spark.openlineage.transport.url", "http://localhost:5000")
    .config("spark.openlineage.namespace", "nyc_taxi_pipeline")
    .getOrCreate())
```
Re-run one of your existing Iceberg read/write jobs and confirm it shows up in Marquez as a distinct job node, ideally connected to the same dataset nodes dbt is touching (this is the payoff — a single graph spanning both tools).

**3c. Airflow → OpenLineage (minimal scaffolding, not the Airflow deep dive)**
Install `apache-airflow-providers-openlineage`, set the `OPENLINEAGE_URL`/`OPENLINEAGE_NAMESPACE` env vars (already in the compose file above), and build **one deliberately simple, linear DAG** — just enough for lineage events to have a job to attach to. No sensors, branching, backfills, or XComs; those belong to your dedicated Airflow project, not this one:

```python
# dags/nyc_taxi_governance_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG("nyc_taxi_governance", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    run_spark_job = PythonOperator(
        task_id="spark_raw_to_iceberg",
        python_callable=lambda: __import__("subprocess").run(
            ["spark-submit", "/opt/spark_jobs/load_iceberg.py"], check=True
        ),
    )
    run_dbt = BashOperator(
        task_id="dbt_build_marts",
        bash_command="cd /opt/dbt/nyc_taxi_dbt && dbt build",
    )
    run_spark_job >> run_dbt
```

This DAG:
1. Runs the Spark job (raw → Iceberg)
2. Triggers `dbt build` (staging → marts)
3. Emits lineage automatically at each task boundary via the provider's auto-instrumentation

Keep this instance running after Phase 5 — it's the environment you'll extend with sensors, branching, and backfills when you get to the Airflow project proper.

**3d. The actual exercise** — deliberately introduce one bad row upstream (e.g. a null zone_id or corrupted fare value in the raw MinIO parquet), run the full DAG, and use Marquez's UI to trace the bad row's *column-level* lineage backward from a failing dbt test through the Spark job to the source dataset. This end-to-end trace is the concrete skill this phase is building — not just "events show up," but "I can debug a real incident using the graph."

---

## Phase 4 — Superset dashboards (Days 10–12)

**4a. Connect Superset to ClickHouse**
```
clickhousedb://default:@localhost:8123/nyc_taxi_marts
```
(needs `clickhouse-sqlalchemy` installed in the Superset container — extend the image or `pip install` into the running container for a local experiment)

**4b. Build 4–5 dashboards**, each exercising a different Superset capability rather than repeating the same chart types:
1. **Trip volume & revenue trends** — time-series line charts off `agg_daily_revenue`
2. **Zone-level heatmap/choropleth** — pickup/dropoff density by taxi zone (tests Superset's geospatial viz)
3. **Fare/tip distribution analysis** — box plots and histograms off `fct_trips` (tests larger-scan performance directly on the fact table)
4. **Materialized view speed comparison dashboard** — build a ClickHouse materialized view that pre-aggregates the same metrics as `agg_daily_revenue`, point one chart at the MV and an identical chart at a live `GROUP BY` over `fct_trips`, and use Superset's query duration display (or `EXPLAIN`/`clickhouse-client` timing) to quantify the difference
5. **Ops/governance dashboard** — row counts and freshness per mart, pass/fail counts from your dbt test and GE runs (if you persist test results to a ClickHouse table, which is a nice small side-project in itself)

**4c. SQL Lab** — spend deliberate time here doing ad-hoc exploration against ClickHouse (not just building charts) — save a few queries, try the query history and "Explore from SQL Lab" chart-creation flow.

**4d. Alerts/Reports** — configure one alert (e.g. "daily row count drops below threshold") and one scheduled report (e.g. emailed/Slack-posted daily revenue dashboard snapshot) using Superset's built-in alerting, no external tool needed. This requires a working SMTP or Slack webhook target — a local mail-catcher container (e.g. `maildev`) is the easiest way to test this without real credentials.

---

## Phase 5 — Wrap-up & synthesis (Days 13–14)

- Write a short retrospective covering: dbt tests vs Great Expectations (your actual conclusion from Phase 2), what column-level lineage in Marquez showed you that logs/print-debugging wouldn't have, and the materialized-view-vs-raw-query performance numbers from Phase 4
- Clean up the docker-compose stack into a single reproducible `docker compose up` for the whole governance layer, with a README documenting service ports and startup order
- Optional stretch: add a second dbt "bad data" scenario and re-run the full Marquez trace to confirm the lineage debugging workflow is repeatable, not a one-off

---

## Suggested day-by-day pacing

| Days | Focus |
|---|---|
| 1 | Environment scaffolding, verify all services boot |
| 2–4 | dbt project: staging → intermediate → marts, dbt tests |
| 5–6 | Great Expectations suites + comparison write-up |
| 7–9 | OpenLineage wiring (dbt, Spark, Airflow) into Marquez, bad-row trace |
| 10–12 | Superset dashboards, SQL Lab, alerting |
| 13–14 | Synthesis, cleanup, retrospective write-up |

## Key risks to watch for
- **dbt-clickhouse** engine/partition syntax differs from other adapters' docs you may find online — lean on the adapter's own repo examples over generic dbt tutorials
- **OpenLineage version compatibility** across dbt, Spark, and Airflow providers is the most common source of silent failures (events not appearing) — pin versions explicitly and check Marquez's raw event log via its API when something doesn't show up in the UI
- **Superset + ClickHouse driver** setup outside the official Docker image sometimes needs a custom image build — plan for a 30-minute detour here
