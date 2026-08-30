# spark_jobs/
#
# Bridge package: scripts that invoke Project 3 (Spark + Iceberg) jobs
# from within this Project 5 (Governance) environment.
#
# Used by:
#   - airflow/dags/nyc_taxi_governance_dag.py  (via run_iceberg_ingest.sh)
#   - Manual runs from WSL2 terminal
