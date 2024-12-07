#!/bin/bash

# Open Docker application (if not already running)
open -a Docker || true
echo "==============Waiting for 5 seconds to Open Docker====================" && sleep 5

# Pull the latest image
docker pull my_jupyter_image

# Stop and remove any running containers with the specified names
docker stop spark-master spark-worker || true
docker rm spark-master spark-worker || true


# Create Docker network for Spark
docker network create spark-network || true  

# Run the Spark master container
docker run -d --network spark-network --name spark-master \
  -p 7077:7077 -p 8080:8080 my_jupyter_image

# Run the Spark worker container
docker run -d --network spark-network --name spark-worker \
  -p 8888:8888 \
  -v /Users/anoopm/Documents/Local_Folder:/mnt \
  -v "/Users/anoopm/Library/CloudStorage/OneDrive-Personal/Cloud Documents:/data" \
  my_jupyter_image

# List running containers
docker ps
echo "=======================All Containers Created================"