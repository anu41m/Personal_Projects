# Use a base image with Jupyter and Spark pre-installed
FROM jupyter/base-notebook:latest

# Set environment variables for Spark, Hadoop, and Delta Lake versions

ENV SPARK_VERSION=3.5.3
ENV DELTA_VERSION=3.2.1
ENV HADOOP_VERSION=3.3.4
ENV MAVEN_ARTIFACT=delta-spark_2.12
ENV JAVA_VERSION=17
ENV PYSPARK_SUBMIT_ARGS="--jars ${MAVEN_ARTIFACT}-${DELTA_VERSION}.jar pyspark-shell"
# Switch to root to install packages
USER root



RUN apt-get update && apt-get install -y \
        curl \
        gnupg \
        openjdk-${JAVA_VERSION}-jdk 

# Install necessary packages and Spark
ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-arm64

COPY Docker_setup/Spark-dependencies/delta-spark_2.12-3.2.1.jar /usr/local/spark/jars/delta-spark_2.12-3.2.1.jar
COPY Docker_setup/Spark-dependencies/spark-3.5.3-bin-hadoop3.tgz /tmp/spark.tgz
RUN tar -xzf /tmp/spark.tgz -C /usr/local && \
mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop3 /usr/local/spark && \
rm /tmp/spark.tgz

# Download and install Hadoop
COPY Docker_setup/Spark-dependencies/hadoop-3.3.4.tar.gz /tmp/hadoop.tar.gz
RUN tar -xzf /tmp/hadoop.tar.gz -C /usr/local/ && \
mv /usr/local/hadoop-${HADOOP_VERSION} /usr/local/hadoop && \
rm /tmp/hadoop.tar.gz 

# Set up permissions for Spark directory and create work directory
RUN mkdir -p /usr/local/spark/work && chown -R jovyan:users /usr/local/spark

# Verify Spark installation
RUN ls -la /usr/local/spark/bin

# Install Delta Lake dependencies
RUN pip install delta-spark==$DELTA_VERSION

# Adjust ownership for Spark directory
RUN chown -R 1000:100 /usr/local/spark

# Install openpyxl for Excel support in PySpark
RUN pip install openpyxl

# Ensure spark-excel JAR is available and correctly placed
RUN ls -la /usr/local/spark/jars/

# Set the working directory for the project
WORKDIR /app

# Copy project files and set permissions
COPY Project-Files /app/Project-Files
RUN chown -R jovyan:users /app && chmod -R 777 /app

# Create directories for data
RUN mkdir -p /mnt/Calendar/Calendar_Parquet && chown -R jovyan:users /mnt/Calendar/Calendar_Parquet

USER jovyan

ENV SPARK_HOME=/usr/local/spark
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin
ENV SPARK_CONF_DIR=$SPARK_HOME/conf

# Copy JAR file and Spark defaults
COPY Docker_setup/fat.jar /app/libs/fat.jar
COPY Docker_setup/spark-master/spark-defaults.json /usr/local/spark/conf/spark-defaults.json
COPY Docker_setup/spark-master/jupyter_notebook_config.py /home/jovyan/.jupyter/
COPY Docker_setup/spark-master/packages.txt /usr/local/spark/conf/packages.txt


# Expose necessary ports
EXPOSE 8888 8080 7077

# Start Spark master and Jupyter Notebook
CMD /bin/bash -c "/usr/local/spark/bin/spark-class org.apache.spark.deploy.master.Master & start-notebook.sh --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=''"