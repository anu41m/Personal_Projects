# Prompt user for input
echo "Enter \"start\" to start the docker image ||\"build\" to build the docker image ||\"scale\" to scale workers||\"stop\" to stop cluster || \"restart\" to restart the cluster:"
read user_input
# Move to the Docker Setup Directory
cd /Users/anoopm/my_jupyter_project/docker_setup || true

# Check the user's input
if [ "$user_input" == "start" ]; then
# Open Docker application (if not already running)
open -a Docker || true
echo "==============Waiting for 5 seconds to Open Docker====================" && sleep 5

# Delete all existing Docker Containers and Networks
docker compose down || true

# Run the Docker Image
docker compose up -d 
# echo "Waiting for Airflow webserver to be healthy..."
# timeout=60  # Timeout after 60 seconds
# elapsed=0
# while ! curl -sf http://127.0.0.1:8080/health; do
#     sleep 5
#     elapsed=$((elapsed + 5))
#     echo "Waiting for Airflow webserver to be healthy... ($elapsed seconds elapsed)"
#     if [ $elapsed -ge $timeout ]; then
#         echo "Timeout waiting for Airflow webserver! Exiting."
#         exit 1
#     fi
# done
# echo "Airflow webserver is ready."
elif [ "$user_input" == "scale" ]; then
    echo "Enter spark-worker number"
    read spark_count
    docker compose up -d --scale spark-worker=$spark_count
    echo "==============Completed==============="
elif [ "$user_input" == "restart" ]; then
    echo "=========== Restarting ==============="
    docker compose restart
    echo "==============Completed==============="
elif [ "$user_input" == "stop" ]; then
    echo "============== Stopping Cluster ======================"
    # Delete all existing Docker Containers and Networks
    docker compose down || true
    echo "=============== Cluster Stopped ===================="
elif [ "$user_input" == "build" ]; then
    docker compose down || true
    docker compose build --progress=plain
else
    echo "Invalid input"
fi