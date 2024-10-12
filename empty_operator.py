from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,11),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG('empty', 
          description='Exemplo de dag com uso do empty_operator',
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime(2024,10,11),
          default_view='graph',
          tags=['estudos', 'email'],
          catchup=False) as dag:

    task1 = BashOperator(task_id='tsk1', bash_command="sleep 2")
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 2")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 2")
    task4 = BashOperator(task_id='tsk4', bash_command="sleep 2")
    task5 = BashOperator(task_id='tsk5', bash_command="sleep 2")
    task_empty = EmptyOperator(task_id='empty')


    [task1, task2, task3] >> task_empty >> [task4, task5]


