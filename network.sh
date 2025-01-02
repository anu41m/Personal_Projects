#!/bin/bash

# Network name
NETWORK_NAME="airflow_b2a6bc_airflow"

# List of container names to connect to the network
CONTAINERS=(
  "spark-master"
  "docker_setup-spark-worker-1"
)

# Check if the network exists
if docker network inspect "$NETWORK_NAME" >/dev/null 2>&1; then
  echo "Network $NETWORK_NAME exists."
else
  echo "Network $NETWORK_NAME does not exist. Creating it..."
  docker network create "$NETWORK_NAME"
fi

# Loop through containers and connect them to the network
for CONTAINER in "${CONTAINERS[@]}"; do
  if docker ps -q -f name="$CONTAINER" >/dev/null; then
    echo "Connecting container $CONTAINER to network $NETWORK_NAME..."
    docker network connect "$NETWORK_NAME" "$CONTAINER"
  else
    echo "Container $CONTAINER is not running or does not exist. Skipping..."
  fi
done

echo "All specified containers have been processed."


echo "Linked Containers"

docker network inspect $NETWORK_NAME