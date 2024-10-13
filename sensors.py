from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.http.sensors.http import HttpSensor
import requests

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,13),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


with DAG('sensors', 
          description='Exemplo de dag que usa um sensor http',
          default_args=default_args,
          schedule_interval=None,
          default_view='graph',
          tags=['estudos', 'sensor'],
          catchup=False) as dag:
    
    def query_api():
        response = requests.get('https://api.publicapis.org/entries')
        print(response.text)

    check_api = HttpSensor(task_id="check_api", http_conn_id="Connection_test", endpoint='entries', poke_interval=2, timeout=20)
    process_data = PythonOperator(task_id="process_data", python_callable=query_api)

    check_api >> process_data