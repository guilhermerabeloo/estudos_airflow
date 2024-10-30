from big_data_operator import BigDataOperator
from datetime import datetime, timedelta
from airflow import DAG

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,11),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG('bigdata', 
          description='Exemplo de dag que usa plugin para transformar arquivo parquet',
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime(2024,10,11),
          default_view='graph',
          tags=['estudos', 'plugin'],
          catchup=False) as dag:
    
    big_data = BigDataOperator(task_id="big_data",
                               path_to_csv_file="/opt/airflow/data/Churn.csv",
                               path_to_save_file="/opt/airflow/data/Churn.parquet",
                               file_type="parquet")

big_data
