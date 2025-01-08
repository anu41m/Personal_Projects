from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from random import randint

def test_model():
    return randint(1, 10)

with DAG("my_dag", start_date=datetime(2020,1,1), schedule_interval=None, catchup=False) as dag:
    
    test=PythonOperator(
        task_id="test",
        python_callable=test_model
        
    )