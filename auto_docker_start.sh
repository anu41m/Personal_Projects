#!/bin/bash

# Pull the latest image
#docker pull my_jupyter_image

# Stop any running container with the same name
docker stop Container_by_Anoop || true

# Remove the container if it exists
docker rm Container_by_Anoop || true

# Identify the Process Using Port 8888 and kill it if necessary
lsof -i :8888 | awk 'NR>1 {print $2}' | xargs kill -9 || true

# Open Docker (Only if needed, this command might vary based on your setup)
open -a Docker

# Build the Docker File
docker build -t my_jupyter_image .

# Run a new container with specified volumes
docker run -d -v /Users/anoopm/Documents/Local_Folder:/mnt -v "/Users/anoopm/Library/CloudStorage/OneDrive-Personal/Cloud Documents":/data -p 8888:8888 --name Container_by_Anoop my_jupyter_image
