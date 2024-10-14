from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,14),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}


def print_xcom_result(ti):
    task_instance = ti.xcom_pull(task_ids="query_data")

    print('RESULTADO:')
    for row in task_instance:
        print(row)


with DAG('pg_provider', 
          description='Exemplo de dag que usa o provider do postgres',
          default_args=default_args,
          schedule_interval=None,
          default_view='graph',
          tags=['estudos', 'sensor'],
          catchup=False) as dag:
    
    create_table = PostgresOperator(task_id="create_table", 
                                    postgres_conn_id="postgres",
                                    sql="create table if not exists teste(id int);")
    
    insert_data = PostgresOperator(task_id="insert_data",
                                   postgres_conn_id="postgres",
                                   sql="insert into teste values(1);")
    
    query_data = PostgresOperator(task_id="query_data",
                                  postgres_conn_id="postgres",
                                  sql="select * from teste;")
    
    print_result = PythonOperator(task_id="print_result",
                                  python_callable=print_xcom_result,
                                  provide_context=True)
    

    create_table >> insert_data >> query_data >> print_result
