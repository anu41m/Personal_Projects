from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
from docker.types import Mount

default_args={
    'owner':'Anoop M'
}

@dag(start_date=datetime(2021,1,1), schedule_interval="32 15 * * *", catchup=False, default_args=default_args)

def dag_Wallet_NTBK():
    # Create the start and stop Spark DAGs by passing valid arguments
    tr_start_spark =TriggerDagRunOperator(
        task_id='tr_start_spark',
        trigger_dag_id='dag_spark_cluster',
        conf={'flag': 1},
    )
    NB_Notebook_job=DockerOperator(
        task_id="NB_Notebook_job",
        image='spark-cluster:version-1.0.0',
        command= 'papermill /home/jovyan/Notebooks/NB_DIM_wallet.ipynb /home/jovyan/Notebooks/output/NB_DIM_wallet.ipynb --log-output',
        docker_url='unix:///var/run/docker.sock',
        mount_tmp_dir=False,
        mounts=[
            Mount(
                target="/data",
                source="/Users/anoopm/Library/CloudStorage/OneDrive-Personal/Cloud_Documents",
                type="bind",
            ),
            Mount(
                target="/mnt",
                source="/Users/anoopm/Documents/Local_Folder",
                type="bind",
            ),
            Mount(
                target="/home/jovyan/Notebooks/output",
                source="/Users/anoopm/my_jupyter_project/Scripts/output",
                type="bind",
            ),
            Mount(
                target="/home/jovyan/Notebooks",
                source="/Users/anoopm/my_jupyter_project/Scripts",
                type="bind",
            ),
        ],
        auto_remove=True,
        network_mode='Airflow_custom_network',
    )
    tr_stop_spark =TriggerDagRunOperator(
        task_id='tr_stop_spark',
        trigger_dag_id='dag_spark_cluster',
        conf={'flag': 0},
        trigger_rule='all_done'
    )
    # Task Dependencies
    tr_start_spark >> NB_Notebook_job >> tr_stop_spark
    
dag = dag_Wallet_NTBK()