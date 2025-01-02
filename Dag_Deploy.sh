#!/bin/bash

# Navigate to your Airflow project directory
cd /Users/anoopm/my_jupyter_project/airflow_testing/ || { echo "Directory not found! Exiting."; exit 1; }

# Define the Airflow webserver container name and source path for DAGs
airflow_webserver="Airflow_Webserver"
source_path="dags/."

echo "========= Copying DAG files to the Airflow webserver ==========="

# Copy the DAG files into the Airflow webserver's DAGs directory
docker cp "$source_path" "$airflow_webserver":/opt/airflow/dags/ || { echo "Failed to copy DAG files into the Airflow webserver's DAGs directory! Exiting."; exit 1; }

docker exec "$airflow_webserver" ls /opt/airflow/dags

# echo "=========== Restarting the Airflow Webserver =================="

# # Restart the Airflow webserver container
# docker compose restart || { echo "Failed to restart Airflow containers! Exiting."; exit 1; }
# docker network create Airflow_custom_network
# docker network connect Airflow_custom_network Airflow_Webserver

# echo "============== Waiting for Airflow webserver to become healthy... ======================"

# # Define the timeout and elapsed time for health check
# timeout=60  # Timeout after 60 seconds
# elapsed=0

# # Check the health status of the Airflow webserver
# while ! curl -sf http://127.0.0.1:8080/health; do
#     sleep 5
#     elapsed=$((elapsed + 5))
#     echo "Waiting for Airflow webserver to become healthy... ($elapsed seconds elapsed)"
#     if [ $elapsed -ge $timeout ]; then
#         echo "Timeout waiting for Airflow webserver! Exiting."
#         exit 1
#     fi
# done

echo "=============== Airflow webserver is ready. ================="