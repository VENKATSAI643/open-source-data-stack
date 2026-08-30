"""
airflow/dags/nyc_taxi_governance_dag.py
────────────────────────────────────────────────────────────────────────────
Minimal linear orchestration DAG for Project 5 (Governance & Serving).

PURPOSE: emit OpenLineage events so Marquez can build a cross-tool lineage
graph that spans Spark → dbt → ClickHouse. This is NOT the Airflow deep-dive.

What is intentionally absent here (saved for the dedicated Airflow project):
  - Sensors (file/table/S3 arrival checks)
  - Branching (conditional task paths)
  - Backfills (historical date range re-processing)
  - XComs (inter-task data passing)
  - TaskGroups, dynamic task mapping, custom operators

DAG flow (deliberately linear):
    spark_raw_to_iceberg  →  dbt_build_marts

Environment requirements (already set in governance-compose.yml):
    OPENLINEAGE_URL=http://marquez:5000
    OPENLINEAGE_NAMESPACE=nyc_taxi_pipeline
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


# ─────────────────────────────────────────────────────────────────────────────
#  Task callables
# ─────────────────────────────────────────────────────────────────────────────

def run_spark_iceberg_job() -> None:
    """
    Triggers the Spark + Iceberg ingestion job from Project 3.

    The shell script at /opt/spark_jobs/run_iceberg_ingest.sh wraps the
    spark-submit call with the OpenLineage listener JAR and config, so Marquez
    receives a Spark job node connected to the same namespace as dbt.

    Raises:
        subprocess.CalledProcessError: if the Spark job exits non-zero.
    """
    result = subprocess.run(
        ["bash", "/opt/spark_jobs/run_iceberg_ingest.sh"],
        capture_output=True,
        text=True,
        check=True,
    )
    print("=== Spark job stdout ===")
    print(result.stdout)
    if result.stderr:
        print("=== Spark job stderr ===")
        print(result.stderr)


# ─────────────────────────────────────────────────────────────────────────────
#  DAG definition
# ─────────────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="nyc_taxi_governance",
    description=(
        "Minimal linear DAG: Spark raw→Iceberg then dbt marts. "
        "Purpose is lineage event emission — not production orchestration."
    ),
    start_date=datetime(2026, 1, 1),
    schedule=None,          # Manual trigger only (no cron schedule)
    catchup=False,
    tags=["governance", "lineage", "project5"],
    doc_md=__doc__,
) as dag:

    # ── Task 1: Spark raw → Iceberg ──────────────────────────────────────────
    # Reads NYC taxi parquet from MinIO raw-zone bucket, writes to Iceberg table.
    # OpenLineage listener emits START/COMPLETE events covering the MinIO → Iceberg hop.
    spark_raw_to_iceberg = PythonOperator(
        task_id="spark_raw_to_iceberg",
        python_callable=run_spark_iceberg_job,
        doc_md=(
            "Invokes Project 3 Spark+Iceberg ingestion job with the OpenLineage "
            "listener. Emits lineage events for the raw parquet → Iceberg table write."
        ),
    )

    # ── Task 2: dbt build marts ──────────────────────────────────────────────
    # Runs staging → intermediate → marts in DAG order.
    # OpenLineage provider auto-instruments each dbt model run as a job node.
    dbt_build_marts = BashOperator(
        task_id="dbt_build_marts",
        bash_command=(
            "source /opt/airflow/dbt_venv/bin/activate && "
            "cd /opt/dbt/nyc_taxi_dbt && dbt-ol build"
        ),
        env={
            **os.environ,
            "OPENLINEAGE_URL": "http://marquez:5000",
            "OPENLINEAGE_NAMESPACE": "nyc_taxi_pipeline",
        },
        doc_md=(
            "Runs dbt build (staging → marts) via dbt-ol wrapper so each model "
            "emits START/COMPLETE lineage events to Marquez."
        ),
    )

    # ── Dependency: linear, no branching ────────────────────────────────────
    spark_raw_to_iceberg >> dbt_build_marts
