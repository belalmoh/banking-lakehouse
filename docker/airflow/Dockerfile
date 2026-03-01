FROM apache/airflow:2.7.0-python3.10

USER root

# Install system dependencies + Java (needed for spark-submit)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    openjdk-17-jre-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment (auto-detect architecture)
RUN ln -s $(dirname $(dirname $(readlink -f $(which java)))) /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# Install Python packages
RUN pip install --no-cache-dir --timeout 300 \
    apache-airflow-providers-apache-spark==4.1.3 \
    delta-spark==2.4.0 \
    pyspark==3.4.0 \
    great-expectations==0.17.19 \
    boto3==1.28.25 \
    dbt-core==1.11.6 \
    dbt-spark==1.10.1

# Set Spark environment
ENV SPARK_HOME=/home/airflow/.local/lib/python3.10/site-packages/pyspark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Replace snappy-java 1.1.9.1 (needs GLIBC 2.32) with 1.1.8.4 (works with GLIBC 2.31)
RUN rm -f ${SPARK_HOME}/jars/snappy-java-*.jar && \
    curl -L -o ${SPARK_HOME}/jars/snappy-java-1.1.8.4.jar \
    https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.8.4/snappy-java-1.1.8.4.jar

# Copy dbt profiles
RUN mkdir -p /home/airflow/.dbt
COPY dbt_profiles.yml /home/airflow/.dbt/profiles.yml
ENV DBT_PROFILES_DIR=/home/airflow/.dbt

# Copy Spark configuration (Delta Lake, S3A, compression settings)
COPY spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf