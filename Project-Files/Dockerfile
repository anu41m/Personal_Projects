# Use a base image with Jupyter and Spark pre-installed
FROM jupyter/all-spark-notebook:latest
# Set environment variables for Spark and Hadoop
ENV SPARK_HOME=/usr/local/spark
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin
ENV PYSPARK_SUBMIT_ARGS="pyspark-shell"
# ENV SPARK_CONF_DIR=$SPARK_HOME/conf

# Set environment variables for Spark and Delta Lake versions
ENV SPARK_VERSION=3.5.3
ENV DELTA_VERSION=3.2.1
ENV HADOOP_VERSION=3.3.4

# Switch to root to install packages
USER root

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    openjdk-11-jdk && \
    curl -L "https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz" -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz && \
    java -version

# Download and install Hadoop manually
RUN curl -L "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" -o /tmp/hadoop.tar.gz && \
    tar -xzf /tmp/hadoop.tar.gz -C /usr/local/ && \
    mv /usr/local/hadoop-$HADOOP_VERSION /usr/local/hadoop && \
    rm /tmp/hadoop.tar.gz

ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Dynamically find JAVA_HOME path
RUN export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment && \
    echo "export JAVA_HOME=$JAVA_HOME" >> ~/.bashrc

    # Set up configuration for Delta Lake
RUN echo "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" >> $SPARK_HOME/conf/spark-defaults.conf
RUN echo "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" >> $SPARK_HOME/conf/spark-defaults.conf

# Verify Spark installation
RUN ls -la /opt/spark/bin

RUN mkdir -p /usr/local/spark/work && chown -R jovyan:users /usr/local/spark

# Install Delta Lake dependencies
RUN pip install delta-spark==$DELTA_VERSION

# Adjust ownership for Spark directory
RUN chown -R 1000:100 /opt/spark

# Set the working directory
WORKDIR /app

# Copy your project files into the container
COPY Project-Files /app/Project-Files

# Set ownership and permissions for app folder
RUN chown -R jovyan:users /app && \
    chmod -R 777 /app

# Switch back to non-root user
USER jovyan

COPY Docker_setup/fat.jar /app/libs/fat.jar


# Copy Spark defaults configuration
COPY Docker_setup/spark-master/spark-defaults.conf /usr/local/spark/conf/

# Copy Jupyter notebook configuration
COPY Docker_setup/spark-master/jupyter_notebook_config.py /home/jovyan/.jupyter/

# Expose ports
EXPOSE 8888 8080 7077




# # Start Spark master and Jupyter Notebook
CMD /bin/bash -c "/usr/local/spark/bin/spark-class org.apache.spark.deploy.master.Master & start-notebook.sh --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=''"
