FROM apache/airflow:2.7.0-python3.10

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.1.3 \
    delta-spark==2.4.0 \
    pyspark==3.4.0 \
    great-expectations==0.17.19 \
    boto3==1.28.25 \
    dbt-core==1.6.0 \
    dbt-spark==1.6.0