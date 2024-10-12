from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
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

with DAG('variaveis', 
          description='Exemplo de dag com uso do variaveis do airflow',
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime(2024,10,11),
          default_view='graph',
          tags=['estudos', 'email'],
          catchup=False) as dag:

    def printaVariavel(**contexto):
        variavel_teste = Variable.get('teste_var')
        print(f'Este é o valor da minha variavel teste: {variavel_teste}')

    task1 = PythonOperator(task_id='tsk1', python_callable=printaVariavel)

    task1


