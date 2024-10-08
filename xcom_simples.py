from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,7,10),
    'email': ['teste@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


def task_write(**kwarg):
    kwarg['ti'].xcom_push(key="valorxcom", value=10200)

def task_read(**kwarg):
    valor = kwarg['ti'].xcom_pull(key="valorxcom")
    print(f'Valor_recuperado: {valor}')

with DAG('xcom_simples', 
          description='Exemplo de dag que usa XCom simples',
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime(2024, 10, 5),
          default_view='graph',
          tags=['estudos', 'xcom'],
          catchup=False) as dag:

    task1 = PythonOperator(task_id='tsk1', python_callable=task_write)
    task2 = PythonOperator(task_id='tsk2', python_callable=task_read)

    task1 >> task2

