#!/bin/bash

# Start Spark Master
$SPARK_HOME/sbin/start-master.sh

# Wait a bit to ensure the master is up
sleep 5

# Start Spark Worker, replace "spark-master" with your master container name if necessary
$SPARK_HOME/sbin/start-worker.sh spark://${SPARK_MASTER_HOST:-spark-master}:7077

exc start-notebook.sh

# Keep the container running
tail -f /dev/null