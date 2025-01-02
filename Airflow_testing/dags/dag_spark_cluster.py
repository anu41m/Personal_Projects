import docker
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args={
    'owner':'Anoop M'
}

# Function to start Spark containers
def start_spark_containers():
    client = docker.from_env()
    spark_master_config = {
        "image": "spark-cluster:version-1.0.0",
        "name": "spark-master",
        "detach": True,
        "ports": {"8080/tcp": 8082, "7077/tcp": 7077},
        "environment": {"SPARK_MODE": "master"},
        "network": "Airflow_custom_network",
        "volumes": {
            "/Users/anoopm/my_jupyter_project/Scripts/output": {"bind": "/home/jovyan/Notebooks/output", "mode": "rw"},
            "/Users/anoopm/my_jupyter_project/Scripts": {"bind": "/home/jovyan/Notebooks", "mode": "rw"},
            "/Users/anoopm/Documents/Local_Folder": {"bind": "/mnt", "mode": "rw"},
            "/Users/anoopm/Library/CloudStorage/OneDrive-Personal/Cloud_Documents": {"bind": "/data", "mode": "rw"},
            "/Users/anoopm/Documents/Spark_Work": {"bind": "/usr/local/spark/work", "mode": "rw"},
            "/Users/anoopm/my_jupyter_project/Docker_setup/spark-master": {"bind": "/usr/local/spark/conf", "mode": "rw"},
        },
    }

    spark_worker_config = {
        "image": "spark-worker:version1.0.0",
        "name": "spark-worker",
        "detach": True,
        "ports": {"8081/tcp": 8081},
        "environment": {"SPARK_MASTER_URL": "spark://spark-master:7077"},
        "network": "Airflow_custom_network",
        "volumes": {
            "/Users/anoopm/my_jupyter_project/Scripts/output": {"bind": "/home/jovyan/Notebooks/output", "mode": "rw"},
            "/Users/anoopm/my_jupyter_project/Scripts": {"bind": "/home/jovyan/Notebooks", "mode": "rw"},
            "/Users/anoopm/Documents/Local_Folder": {"bind": "/mnt", "mode": "rw"},
            "/Users/anoopm/Library/CloudStorage/OneDrive-Personal/Cloud_Documents": {"bind": "/data", "mode": "rw"},
            "/Users/anoopm/Documents/Spark_Work": {"bind": "/usr/local/spark/work", "mode": "rw"},
            "/Users/anoopm/my_jupyter_project/Docker_setup/spark-master": {"bind": "/usr/local/spark/conf", "mode": "rw"},
        },
    }

    try:
        client.containers.run(**spark_master_config)
        print("Spark Master started.")
    except docker.errors.APIError as e:
        print(f"Error starting Spark Master: {e}")

    try:
        client.containers.run(**spark_worker_config)
        print("Spark Worker started.")
    except docker.errors.APIError as e:
        print(f"Error starting Spark Worker: {e}")

# Function to stop Spark containers
def stop_spark_containers():
    client = docker.from_env()
    for container in ["spark-master", "spark-worker"]:
        try:
            client.containers.get(container).stop()
            client.containers.get(container).remove()
            print(f"{container} removed")
        except docker.errors.NotFound:
            print(f"{container} not running")

# Reusable DAG generator
def create_spark_dag(dag_id, start_date, flag):
    with DAG(dag_id=dag_id, schedule_interval=None, start_date=start_date, catchup=False, default_args=default_args) as dag:
        def spark_action(**kwargs):
            flag= kwargs['dag_run'].conf.get('flag', 0)
            if flag == 1:
                start_spark_containers()
            else:
                stop_spark_containers()
            
        spark_action=PythonOperator(
            task_id='spark_action',
            python_callable=spark_action,
            provide_context=True,
        )
    return dag

# # Now, create a DAG with the proper arguments
dag = create_spark_dag('dag_spark_cluster', datetime(2021, 1, 1),0)