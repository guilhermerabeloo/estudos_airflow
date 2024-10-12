from airflow import DAG
from airflow.operators.bash import BashOperator

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

with DAG('pool', 
          description='Exemplo de dag com uso do pool e priority',
          default_args=default_args,
          schedule_interval=None,
          default_view='graph',
          tags=['estudos', 'email', 'pool', 'priority'],
          catchup=False) as dag:

    task1 = BashOperator(task_id='tsk1', bash_command="sleep 2", pool="pool_teste")
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 2", pool="pool_teste", priority_weight=5)
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 2", pool="pool_teste")
    task4 = BashOperator(task_id='tsk4', bash_command="sleep 2", pool="pool_teste", priority_weight=10)


