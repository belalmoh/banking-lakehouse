FROM jupyter/pyspark-notebook:spark-3.4.1

USER root

# Install system dependencies if needed
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Download Hadoop AWS and Delta Lake JARs into Spark's jars directory
ENV SPARK_JARS_DIR=/usr/local/spark/jars
RUN curl -L -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-core_2.12-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar && \
    curl -L -o ${SPARK_JARS_DIR}/delta-storage-2.4.0.jar \
    https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar

USER jovyan

# Install Delta Lake and other dependencies
RUN pip install --no-cache-dir \
    delta-spark==2.4.0 \
    great-expectations==0.17.19 \
    boto3==1.28.25 \
    dbt-core==1.6.0 \
    dbt-spark==1.6.0
