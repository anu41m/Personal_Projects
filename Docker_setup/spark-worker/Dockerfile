FROM jupyter/all-spark-notebook:latest 

# Set environment variables for Spark and Hadoop
ENV HADOOP_HOME=/usr/local/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$SPARK_HOME/bin:$HADOOP_HOME/bin

# Install Java and set JAVA_HOME
USER root

# Create a Conda environment with Python 3.12 and activate it
# RUN echo "source /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
#     /bin/bash -c "source /opt/conda/etc/profile.d/conda.sh && \
#                   conda create -y -n py312 -c conda-forge python=3.12 && \
#                   conda activate py312 && \
#                   mamba install -y numpy pandas && \
#                   mamba clean -a -y"

ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
# Install Java for Hadoop compatibility
RUN apt-get update && apt-get install -y \
    telnet\
    curl \
    gnupg \
    openjdk-17-jdk\
    && curl -o /tmp/spark.tgz "https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz" \
    && tar -xvzf /tmp/spark.tgz -C /usr/local \
    && rm -rf /usr/local/spark\
    && mv /usr/local/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /usr/local/spark \
    && rm /tmp/spark.tgz
    
# Set environment variables
ENV SPARK_HOME=/usr/local/spark
ENV PATH="$SPARK_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=/opt/conda/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/conda/bin/python


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Create Spark work directory
RUN mkdir -p ${SPARK_HOME}/work /mnt /data /home/jovyan/Notebooks/output &&\
    chmod -R 777 /mnt /data /home/jovyan/Notebooks &&\
    chown -R jovyan:users ${SPARK_HOME} /home/jovyan/.jupyter /mnt /data /home/jovyan/Notebooks


# Switch back to jovyan user
USER jovyan

VOLUME [ "/Users/anoopm/Documents/Local_Folder:/mnt","/Users/anoopm/Library/CloudStorage/OneDrive-Personal/Cloud_Documents:/data" ]

# Expose necessary ports for worker Web UI and communication
EXPOSE 8081 7337

# Start Spark worker and connect to master
CMD ["/bin/bash", "-c", "${SPARK_HOME}/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077"]