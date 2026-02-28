# =============================================================================
# Spark Dockerfile with Delta Lake Support
# Banking Lakehouse Project
# =============================================================================
FROM bitnamilegacy/spark:3.4.3-debian-12-r9

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Download Delta Lake and Hadoop AWS JARs
ENV DELTA_CORE_VERSION=2.4.0
ENV HADOOP_AWS_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.262
ENV SPARK_JARS_DIR=/opt/bitnami/spark/jars

RUN curl -L -o ${SPARK_JARS_DIR}/delta-core_2.12-${DELTA_CORE_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/${DELTA_CORE_VERSION}/delta-core_2.12-${DELTA_CORE_VERSION}.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-storage-${DELTA_CORE_VERSION}.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_CORE_VERSION}/delta-storage-${DELTA_CORE_VERSION}.jar && \
    curl -L -o ${SPARK_JARS_DIR}/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -L -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

# Install Python packages
RUN pip install --no-cache-dir \
    delta-spark==2.4.0 \
    pandas==2.0.3 \
    great-expectations==0.17.19 \
    faker==19.3.0 \
    boto3==1.28.25 \
    pyarrow==12.0.1 \
    python-dotenv==1.0.0 \
    loguru==0.7.0 \
    dbt-core==1.11.6 \
    dbt-spark==1.10.1

# Copy Spark configuration
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

# Copy dbt profiles
RUN mkdir -p /home/spark/.dbt
COPY dbt_profiles.yml /home/spark/.dbt/profiles.yml
ENV DBT_PROFILES_DIR=/home/spark/.dbt

USER 1001