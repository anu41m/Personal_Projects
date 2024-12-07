# Use a base image with Jupyter and Spark pre-installed
FROM jupyter/all-spark-notebook:latest

# Set environment variables for Spark and Hadoop
ENV SPARK_HOME=/usr/local/spark
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin

# Install Java and set JAVA_HOME
USER root
RUN apt-get update && apt-get install -y openjdk-11-jdk

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64

# Create Spark work directory
RUN mkdir -p /usr/local/spark/work && chown -R jovyan:users /usr/local/spark

# Switch back to jovyan user
USER jovyan

# Expose necessary ports for worker Web UI and communication
EXPOSE 8081 7337

# Start Spark worker and connect to master
CMD ["/bin/bash", "-c", "sleep 10 && /usr/local/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077"]