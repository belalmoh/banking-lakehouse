# =============================================================================
# Airflow Dockerfile for Banking Lakehouse
# =============================================================================
FROM apache/airflow:2.7.0-python3.9

USER root

# Install system dependencies required by dbt and PySpark
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libsasl2-dev \
    openjdk-11-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.5 \
    apache-airflow-providers-postgres==5.6.0 \
    delta-spark==2.4.0 \
    pyspark==3.4.0 \
    great-expectations==0.17.19 \
    dbt-core==1.6.0 \
    dbt-spark==1.6.0 \
    boto3==1.28.25 \
    loguru==0.7.0
