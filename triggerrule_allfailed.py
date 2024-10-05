from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('trigger_allfailed', 
          description='Exemplo de dag com a trigger rule All_Failed',
          schedule_interval=None,
          start_date=datetime(2024, 10, 5),
          catchup=False) as dag:

    task1 = BashOperator(task_id='tsk1', bash_command="exit 1")
    task2 = BashOperator(task_id='tsk2', bash_command="exit 1")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 5", trigger_rule="all_failed")


    [task1, task2] >> task3 

