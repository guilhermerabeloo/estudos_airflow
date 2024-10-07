from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG('dag_to_dag1', 
          description='Exemplo de dag que executa outra dag',
          schedule_interval=None,
          start_date=datetime(2024, 10, 5),
          catchup=False) as dag:

    task1 = BashOperator(task_id='tsk1', bash_command="sleep 4")
    task2 = TriggerDagRunOperator(task_id='tsk2', trigger_dag_id="dag_to_dag2")

    task1 >> task2

