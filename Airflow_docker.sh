#!/bin/bash

# Navigate to your Airflow project directory
cd /Users/anoopm/my_jupyter_project/airflow_docker || { echo "Directory not found! Exiting."; exit 1; }
# Open Docker application (if not already running)
echo "Enter \"start\" to start the server||\"stop\" to stop server ||\"build\" to build image || \"restart\" to restart the server:"
read user_input

# Set AIRFLOW_UID in .env if not already present
if ! grep -q "AIRFLOW_UID=" .env; then
  echo "AIRFLOW_UID=$(id -u)" >> .env
fi

if [ "$user_input" = "start" ]; then
    # Initialize the Airflow database
    echo "Initializing Airflow database..."
    docker-compose run --rm airflow-init || { echo "Failed to initialize Airflow database! Exiting."; exit 1; }

    # Build and start the Airflow containers
    docker-compose up -d || { echo "Failed to start Airflow containers! Exiting."; exit 1; }

    # Wait for the webserver to be healthy
    echo "Waiting for Airflow webserver to be healthy..."
    until curl -sf http://127.0.0.1:8081/health; do
    sleep 5
    echo "Waiting for Airflow webserver to be healthy..."
    done
    echo "Airflow webserver is ready."

    # Optional: Start the scheduler (not needed if already started with `docker-compose up -d`)
    echo "Airflow scheduler is running."
elif [ "$user_input" = "restart" ]; then
    echo "=========== Restarting ==============="
    docker-compose restart
    echo "==============Completed==============="
elif [ "$user_input" = "stop" ]; then
    echo "============== Stopping Server ======================"
    # Delete all existing Docker Containers and Networks
    docker-compose down || true
    echo "=============== Server Down ===================="
elif [ "$user_input" = "build" ]; then
    echo "Building Docker images..."
    docker-compose build || { echo "Failed to build Docker images! Exiting."; exit 1; }

else
    echo "Invalid input! Please enter \"start\", \"stop\", \"restart\", or \"build\"."
    exit 1
fi