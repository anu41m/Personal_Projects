FROM quay.io/astronomer/astro-runtime:12.4.0
# Copy custom configuration into the Airflow image
USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

COPY Spark-Config/spark_config.json /opt/airflow/config/spark_config.json
COPY notebooks /opt/airflow/notebooks

USER airflow