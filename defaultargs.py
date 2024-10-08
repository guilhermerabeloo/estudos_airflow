from airflow import DAG
from airflow.operators.bash import BashOperator
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

with DAG('default_args', 
          description='Exemplo de dag que usa o default args',
          default_args=default_args,
          schedule_interval='@daily',
          start_date=datetime(2024, 10, 5),
          default_view='graph',
          tags=['estudos', 'with_args'],
          catchup=False) as dag:

    task1 = BashOperator(task_id='tsk1', bash_command="sleep 4", retries=3)
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 4")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 4")

    task1 >> task2 >> task3

