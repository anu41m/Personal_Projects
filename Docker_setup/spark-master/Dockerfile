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


ENV HADOOP_VERSION=3.2.4
# Install Java for Hadoop compatibility

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    openjdk-17-jdk\
    && pip install delta-spark==$DELTA_VERSION papermill openpyxl yfinance\
    && curl -o /tmp/hadoop.tar.gz "https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz"\
    && tar -xzf /tmp/hadoop.tar.gz -C /usr/local/ && \
    mv /usr/local/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm /tmp/hadoop.tar.gz 
    
# Set environment variables
ENV PYSPARK_PYTHON=/opt/conda/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python
ENV PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-spark_2.12:3.1.0 pyspark-shell"

# Set the JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# # Copy configuration files to the base Spark directories
# COPY Docker_setup/spark-master/spark-defaults.json ${SPARK_HOME}/conf/spark-defaults.json
# COPY Docker_setup/spark-master/jupyter_notebook_config.py /home/jovyan/.jupyter/
# COPY Docker_setup/spark-master/packages.txt ${SPARK_HOME}/conf/packages.txt
# COPY Docker_setup/fat.jar /app/setup/fat.jar
# # COPY /Scripts /home/jovyan/Notebooks

# Adjust permissions for added files
RUN mkdir -p /mnt /data /home/jovyan/Notebooks /home/jovyan/Notebooks/output &&\
    chmod -R 777 /home/jovyan/Notebooks/output &&\
    chown -R jovyan:users ${SPARK_HOME} ${HADOOP_HOME} /home/jovyan/.jupyter /home/jovyan/Notebooks /home/jovyan/Notebooks/output

# Expose necessary ports
EXPOSE 8888 8082 7077

# Switch back to the default user
USER jovyan

# Start Spark master and Jupyter Notebook
#CMD ["/bin/bash", "-c", "${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.master.Master & start-notebook.sh --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=''"]