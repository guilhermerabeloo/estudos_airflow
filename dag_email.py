from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,9),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG('dag_email', 
          description='Exemplo de dag que envia email em caso de erro',
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime(2024,10,9),
          default_view='graph',
          tags=['estudos', 'email'],
          catchup=False) as dag:

    task1 = BashOperator(task_id='tsk1', bash_command="sleep 2")
    task2 = BashOperator(task_id='tsk2', bash_command="sleep 2")
    task3 = BashOperator(task_id='tsk3', bash_command="sleep 2")
    task4 = BashOperator(task_id='tsk4', bash_command="Exit 1")
    task5 = BashOperator(task_id='tsk5', bash_command="sleep 2", trigger_rule="none_failed")
    task6 = BashOperator(task_id='tsk6', bash_command="sleep 2", trigger_rule="none_failed")

    task_email = EmailOperator(task_id='tsk_email', 
                               to='guilhermerabelo699@gmail.com',
                               subject="Airflow de Guilherme - Task email",
                               html_content="""
                                        <h3>Exemplo de email de erro</h3>
                                    """,
                               trigger_rule="one_failed"
                               )

    [task1, task2] >> task3 >> task4
    task4 >> [task5, task6, task_email]

