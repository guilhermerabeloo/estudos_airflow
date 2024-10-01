from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG('paralelismo', 
          description='Exemplo de dags executando em paralelo',
          schedule_interval=None,
          start_date=datetime(2023, 9, 30),
          catchup=False)

task1 = BashOperator(task_id='tsk1', bash_command="sleep 5", dag=dag)
task2 = BashOperator(task_id='tsk2', bash_command="sleep 5", dag=dag)
task3 = BashOperator(task_id='tsk3', bash_command="sleep 5", dag=dag)


task1 >> [task2, task3]

