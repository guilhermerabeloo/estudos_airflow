from airflow import DAG
from airflow import Dataset 
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,13),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG('producer', 
          description='Exemplo de dag com uso que dispara um dataset',
          default_args=default_args,
          schedule_interval=None,
          default_view='graph',
          tags=['estudos', 'python_operator'],
          catchup=False) as dag:
    
    dataset = Dataset("/opt/airflow/data/Churn_new.csv")

    def novo_arquivo():
        dataset_original = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")        
        dataset_original.to_csv("/opt/airflow/data/Churn_new.csv", sep=";") 

    task1 = PythonOperator(task_id="tsk1", python_callable=novo_arquivo, outlets=[dataset])

    task1