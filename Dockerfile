# Use an official Jupyter Notebook image
FROM jupyter/scipy-notebook:latest

# Set the working directory
WORKDIR /home/jovyan/work

# Switch to root user to install packages
USER root

# Install Java, PySpark, and Google Cloud SDK
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y python3-pip && \
    pip3 install pyspark pandasql google-cloud-storage && \
    apt-get install -y apt-transport-https ca-certificates gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
    apt-get update && \
    apt-get install -y google-cloud-sdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH="$JAVA_HOME/bin:$PATH"

# Verify Java installation
RUN java -version

VOLUME [ "/mnt","/data" ]

# Switch back to jovyan user
USER jovyan

# Copy the local notebook to the container
COPY ./test.ipynb /home/jovyan/work/test.ipynb

# Expose port 8888 for Jupyter
EXPOSE 8888

# Start Jupyter Notebook automatically
CMD ["start-notebook.sh", "--NotebookApp.token=''"]
