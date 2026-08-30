"""
spark_jobs/openlineage_spark_config.py
────────────────────────────────────────────────────────────────────────────
Provides a SparkSession builder pre-wired with the OpenLineage listener.

Use this when running Spark jobs MANUALLY from WSL2 to emit lineage events
to Marquez without going through the Airflow DAG.

Usage:
    from spark_jobs.openlineage_spark_config import get_ol_spark_session

    spark = get_ol_spark_session(app_name="my-manual-job")
    spark.sql("SELECT ...")
    spark.stop()

The JAR is downloaded once via Spark's --packages mechanism and cached in
~/.ivy2/cache — subsequent runs use the local cache (no re-download).

Version pinning note:
    OL_JAR_VERSION must match the openlineage-dbt and openlineage-python
    package versions installed in ~/venvs/governance to avoid silent
    event format mismatches. Pin all three to 1.29.0.
"""
from __future__ import annotations

import os
from pyspark.sql import SparkSession

# ── Version pins — keep in sync with requirements in governance venv ──────────
OL_JAR_VERSION = "1.29.0"
SPARK_SCALA    = "2.12"

OL_JAR = f"io.openlineage:openlineage-spark_{SPARK_SCALA}:{OL_JAR_VERSION}"

# ── OpenLineage transport config — reads from env or falls back to defaults ──
MARQUEZ_URL  = os.getenv("OPENLINEAGE_URL",       "http://localhost:5000")
OL_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "nyc_taxi_pipeline")


def get_ol_spark_session(app_name: str = "nyc-taxi-ol-manual") -> SparkSession:
    """
    Builds a SparkSession with the OpenLineage listener configured.

    All OpenLineage configs are layered on top of whatever SparkSession
    is already configured in config/spark_session.py from Project 3.
    getOrCreate() means multiple calls in the same Python process are safe.

    Args:
        app_name: Application name shown in Spark UI and in Marquez job name.

    Returns:
        A SparkSession with the OpenLineage listener registered.

    Example:
        >>> spark = get_ol_spark_session("schema-evolution-manual")
        >>> spark.sql("SELECT snapshot_id FROM local.taxi.trips.snapshots").show()
        >>> spark.stop()
    """
    return (
        SparkSession.builder
        .appName(app_name)
        # ── OpenLineage listener ────────────────────────────────────────────
        .config("spark.jars.packages", OL_JAR)
        .config(
            "spark.extraListeners",
            "io.openlineage.spark.agent.OpenLineageSparkListener",
        )
        .config("spark.openlineage.transport.type",    "http")
        .config("spark.openlineage.transport.url",     MARQUEZ_URL)
        .config("spark.openlineage.namespace",         OL_NAMESPACE)
        .config("spark.openlineage.appName",           app_name)
        .getOrCreate()
    )


if __name__ == "__main__":
    # Quick connectivity smoke test
    spark = get_ol_spark_session("ol-smoke-test")
    print(f"SparkSession ready | OL URL={MARQUEZ_URL} | namespace={OL_NAMESPACE}")
    spark.sql("SELECT 1 AS ping").show()
    spark.stop()
    print("OpenLineage smoke test complete — check Marquez for the job node.")
