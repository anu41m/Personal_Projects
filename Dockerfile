# Use a base image with Jupyter and Spark pre-installed
FROM jupyter/all-spark-notebook:latest

# Set environment variables for Spark, Hadoop, and Delta Lake versions
ENV SPARK_HOME=/usr/local/spark
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin
ENV PYSPARK_SUBMIT_ARGS="--jars pyspark-shell"
ENV SPARK_CONF_DIR=$SPARK_HOME/conf
ENV SPARK_VERSION=3.5.3
ENV DELTA_VERSION=3.0.0
ENV HADOOP_VERSION=3.3.4

# Switch to root to install packages
USER root

# Install necessary packages and Spark
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    openjdk-11-jdk && \
    export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    # Debug Spark installation
    curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

# Download and install Hadoop
RUN curl -L "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" -o /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /usr/local/ && \
    mv /usr/local/hadoop-${HADOOP_VERSION} /usr/local/hadoop && \
    rm /tmp/hadoop.tar.gz

# Set up permissions for Spark directory and create work directory
RUN mkdir -p /usr/local/spark/work && chown -R jovyan:users /usr/local/spark

# Verify Spark installation
RUN ls -la /opt/spark/bin

# Install Delta Lake dependencies
RUN pip install delta-spark==$DELTA_VERSION

# Adjust ownership for Spark directory
RUN chown -R 1000:100 /opt/spark

# Install openpyxl for Excel support in PySpark
RUN pip install openpyxl

# Ensure spark-excel JAR is available and correctly placed
RUN ls -la /opt/spark/jars/

# Set the working directory for the project
WORKDIR /app

# Copy project files and set permissions
COPY Project-Files /app/Project-Files
RUN chown -R jovyan:users /app && chmod -R 777 /app

# Create directories for data
RUN mkdir -p /mnt/Calendar/Calendar_Parquet && chown -R jovyan:users /mnt/Calendar/Calendar_Parquet

# Copy JAR file and Spark defaults
COPY Docker_setup/fat.jar /app/libs/fat.jar
COPY Docker_setup/spark-master/spark-defaults.conf /usr/local/spark/conf/
COPY Docker_setup/spark-master/jupyter_notebook_config.py /home/jovyan/.jupyter/

# Expose necessary ports
EXPOSE 8888 8080 7077

# Start Spark master and Jupyter Notebook
CMD /bin/bash -c "/usr/local/spark/bin/spark-class org.apache.spark.deploy.master.Master & start-notebook.sh --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=''"