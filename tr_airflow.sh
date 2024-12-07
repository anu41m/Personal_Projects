#!/bin/bash

# Navigate to your Airflow project directory
cd /Users/anoopm/my_jupyter_project/Airflow || { echo "Directory not found! Exiting."; exit 1; }

webserver_health(){
    echo "Waiting for Airflow webserver to be healthy..."
    timeout=60  # Timeout after 60 seconds
    elapsed=0
    while ! curl -sf http://127.0.0.1:8080/health; do
        sleep 5
        elapsed=$((elapsed + 5))
        echo "Waiting for Airflow webserver to be healthy... ($elapsed seconds elapsed)"
        if [ $elapsed -ge $timeout ]; then
            echo "Timeout waiting for Airflow webserver! Exiting."
            exit 1
        fi
    done
    echo "Airflow webserver is ready."
    }

echo "Enter \"start\" to start the server || \"stop\" to stop the server || \"restart\" to restart the server:"
read user_input

# Ensure Astronomer CLI is installed
if ! command -v astro &> /dev/null; then
    echo "Astronomer CLI not found! Please install it and try again."
    exit 1
fi
case "$user_input" in
    "start")
echo "Starting Airflow containers..."
astro dev start || { echo "Failed to start Airflow containers! Exiting."; exit 1; }
webserver_health
;;
"restart")
echo "=========== Restarting Airflow containers ==============="
echo "Restarting Airflow containers..."
astro dev restart || { echo "Failed to restart Airflow containers! Exiting."; exit 1; }
webserver_health
echo "============== Restart Completed ==============="
;;
"stop")
echo "============== Stopping Server ======================"
astro dev stop || { echo "Failed to stop Airflow containers! Exiting."; exit 1; }
echo "=============== Server Down ===================="
;;
*)
echo "Invalid input! Please enter \"start\", \"stop\", or \"restart\"."
;;
esac