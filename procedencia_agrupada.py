from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.utils.task_group import TaskGroup

with DAG('procedencia_agrupada', 
          description='Exemplo de dag com agrupamento no fluxo',
          schedule_interval=None,
          start_date=datetime(2024, 10, 5),
          catchup=False) as dag:

    task1 = BashOperator(task_id='tsk1', bash_command="sleep 4")
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 4")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 4")
    task4 = BashOperator(task_id='tsk4', bash_command="sleep 4")
    task5 = BashOperator(task_id='tsk5', bash_command="sleep 4")
    task6 = BashOperator(task_id='tsk6', bash_command="sleep 4")


    taskgroup = TaskGroup("tsk_group")  

    task7 = BashOperator(task_id='tsk7', bash_command="sleep 4", task_group=taskgroup)
    task8 = BashOperator(task_id='tsk8', bash_command="sleep 4", task_group=taskgroup)
    task9 = BashOperator(task_id='tsk9', bash_command="sleep 4", task_group=taskgroup, trigger_rule="one_failed")

    task1 >> task2
    task3 >> task4
    [task2, task4] >> task5 >> task6
    task6 >> taskgroup

