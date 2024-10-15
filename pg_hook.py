from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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


with DAG('pg_hook', 
          description='Exemplo de dag que usa o hook do postgres',
          default_args=default_args,
          schedule_interval=None,
          default_view='graph',
          tags=['estudos', 'hook'],
          catchup=False) as dag:

    def create_table():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        pg_hook.run("create table if not exists teste2(id int);", autocommit=True)

    def insert_data():
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        pg_hook.run('insert into teste2 values(1);', autocommit=True)

    def query_data(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        records = pg_hook.get_records('select * from teste2;')
        kwargs['ti'].xcom_push(key="query_result", value=records)

    def print_data(ti):
        task_instance = ti.xcom_pull(key="query_result", task_ids="query_data_task")
        print("DADOS DA TABELA:")
        for row in task_instance:
            print(row)

    create_table_task = PythonOperator(task_id="create_table_task", python_callable=create_table)
    insert_data_task = PythonOperator(task_id="insert_data_task", python_callable=insert_data)
    query_data_task = PythonOperator(task_id="query_data_task", python_callable=query_data, provide_context=True)
    print_data_task = PythonOperator(task_id="print_data_task", python_callable=print_data, provide_context=True)

    create_table_task >> insert_data_task >> query_data_task >> print_data_task