FROM apache/airflow:2.10.2-python3.11

# Switch to root to install OS-level dependencies
USER root

# Install Java (required for PySpark)
RUN apt-get update && \
    apt-get install -y default-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Switch back to airflow user to install Python packages
USER airflow

# Install PySpark in the main Airflow environment using Airflow constraints to prevent corruption!
ARG AIRFLOW_VERSION="2.10.2"
ARG PYTHON_VERSION="3.11"
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
RUN pip install --no-cache-dir "pyspark==3.5.2" --constraint "${CONSTRAINT_URL}"

# Create an isolated virtual environment for dbt to avoid protobuf and typing-extensions conflicts
RUN python -m venv /opt/airflow/dbt_venv && \
    . /opt/airflow/dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-clickhouse==1.9.3 openlineage-dbt
