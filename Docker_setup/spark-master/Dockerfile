FROM jupyter/all-spark-notebook:latest 

# Set environment variables for Delta Lake and PySpark
ENV DELTA_VERSION=3.1.0
ENV MAVEN_ARTIFACT=delta-spark_2.12
ENV MAVEN_CONTRIBS=delta-contribs_2.12
ENV MAVEN_STORAGE=delta-storage
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

# Switch to root user to modify the base image
USER root

# Copy the pom.xml to the Docker image
COPY Docker_setup/spark-master/pom.xml /app/pom.xml


ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
# Install Java for Hadoop compatibility
COPY Docker_setup/Spark-dependencies/spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz
COPY Docker_setup/Spark-dependencies/hadoop-3.3.4.tar.gz /tmp/hadoop.tar.gz
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    openjdk-17-jdk\
    && tar -xvzf /tmp/spark.tgz -C /usr/local \
    && rm -rf /usr/local/spark\
    && mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm /tmp/spark.tgz\
    && pip install delta-spark==$DELTA_VERSION papermill openpyxl yfinance\
    && tar -xzf /tmp/hadoop.tar.gz -C /usr/local/ && \
    mv /usr/local/hadoop-3.3.4 ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz 
    
# Set environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=/opt/conda/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0 pyspark-shell"

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Copy configuration files to the base Spark directories
COPY Docker_setup/spark-master/spark-defaults.json /usr/local/spark/conf/spark-defaults.json
COPY Docker_setup/spark-master/jupyter_notebook_config.py /home/jovyan/.jupyter/
COPY Docker_setup/spark-master/packages.txt /usr/local/spark/conf/packages.txt
COPY Docker_setup/fat.jar /app/setup/fat.jar
# COPY /Scripts /home/jovyan/Notebooks

VOLUME [ "/Users/anoopm/my_jupyter_project/Scripts/output:/home/jovyan/Notebooks/output" ]

# Adjust permissions for added files
RUN mkdir -p /mnt /data /home/jovyan/Notebooks /home/jovyan/Notebooks/output &&\
    chmod -R 777 /home/jovyan/Notebooks/output &&\
    chown -R jovyan:users ${SPARK_HOME} ${HADOOP_HOME} /home/jovyan/.jupyter /home/jovyan/Notebooks /home/jovyan/Notebooks/output

VOLUME [ "/Users/anoopm/Documents/Local_Folder:/mnt","/Users/anoopm/Library/CloudStorage/OneDrive-Personal/Cloud_Documents:/data" ]

# Expose necessary ports
EXPOSE 8888 8082 7077

# Switch back to the default user
USER jovyan

# Start Spark master and Jupyter Notebook
CMD ["/bin/bash", "-c", "${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master & start-notebook.sh --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=''"]