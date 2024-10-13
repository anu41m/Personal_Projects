#!/bin/bash

# Pull the latest image
#docker pull my_jupyter_image

# Stop any running container with the same name
docker stop Container_by_Anoop || true

# Remove the container if it exists
docker rm Container_by_Anoop || true