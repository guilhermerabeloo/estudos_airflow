from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,11),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG('branch_operator', 
          description='Exemplo de dag com uso do branch_operator',
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime(2024,10,11),
          default_view='graph',
          tags=['estudos', 'email'],
          catchup=False) as dag:
    
    def geraNumeroAleatorio():
        return random.randint(1,10)
    
    task_geraNumeroAleatorio = PythonOperator(task_id="task_geraNumeroAleatorio",python_callable=geraNumeroAleatorio)

    def avaliaNumAleatorio(**args):
        numero = args['task_instance'].xcom_pull(task_ids="task_geraNumeroAleatorio")
        if numero % 2 == 0:
            return 'task_par'
        else:
            return 'task_impar'

    task_branch = BranchPythonOperator(task_id="task_branch", python_callable=avaliaNumAleatorio, provide_context=True)

    task_par = BashOperator(task_id='task_par', bash_command='echo "Numero par"')
    task_impar = BashOperator(task_id='task_impar', bash_command='echo "Numero impar"')

    task_geraNumeroAleatorio >> task_branch
    task_branch >> task_par
    task_branch >> task_impar
