import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 

dag = DAG (
    dag_id = "Sparktest",
    default_args={
        "owner" : "Anoop M",
        "start_date" : airflow.utils.dates.days_ago(1 ) 
    },
    schedule_interval = "@daily" 
    
)

start = PythonOperator(
    task_id = "start",
    python_callable = lambda: print("Jobs Started"),
    dag = dag
)

python_job = SparkSubmitOperator(
    task_id = "python_job",
    conn_id = "spark-conn", 
    application = "/opt/airflow/notebooks/simple_prgm.py",
    dag = dag
)

end = PythonOperator(
    task_id = "end",
    python_callable = lambda: print("Jobs Ended"),
    dag = dag
)


start >> python_job >> end
