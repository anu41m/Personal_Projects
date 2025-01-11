docker build -t anoopm2801/spark-cluster:version-1.0.0 -f Docker_setup/spark-master/Dockerfile .
docker push anoopm2801/spark-cluster:version-1.0.0

docker build -t anoopm2801/spark-worker:version-1.0.0 -f Docker_setup/spark-worker/Dockerfile .
docker push anoopm2801/spark-worker:version-1.0.0