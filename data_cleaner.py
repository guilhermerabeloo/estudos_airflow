from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import statistics as sts

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024,10,11),
    'email': ['guilhermerabelo699@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

with DAG('data_cleaner', 
          description='Exemplo de dag com uso do python_operator para fazer uma limpeza de dados em um csv',
          default_args=default_args,
          schedule_interval=None,
          start_date=datetime(2024,10,11),
          default_view='graph',
          tags=['estudos', 'python_operator'],
          catchup=False) as dag:
    
    def dataCleaner():
        dataset = pd.read_csv("/opt/airflow/data/Churn.csv", sep=";")
        dataset.columns = ["Id", "Score", "Estado", "Genero", "Idade", "Patrimonio", "Saldo", "Produtos", "TemCartCredito", "Ativo", "Salario", "Saiu"]

        mediana = sts.median(dataset["Salario"])

        dataset["Salario"].fillna(mediana, inplace=True)

        dataset["Genero"].fillna("Masculino", inplace=True)

        mediana = sts.median(dataset["Idade"])
        dataset.loc[(dataset["Idade"]<0) | (dataset["Idade"] > 120), "Idade"] = mediana

        dataset.drop_duplicates(subset="Id", keep="first", inplace=True)

        dataset.to_csv("/opt/airflow/data/Churn_clean.csv", sep=";", index=False)

    task1 = PythonOperator(task_id="task1", python_callable=dataCleaner )
