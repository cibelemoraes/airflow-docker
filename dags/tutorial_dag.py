from airflow import DAG
from datetime import datetime

# procurarsaber qual o operator para glogle cloud e postgre
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as np
import requests
import json


# esse função captura um arquivo json da web transforma em um data frame, lee as linhas do dataframe e retorna a quantidade
def captura_conta_dados():
    url = "https://data.city.........ur.json"
    response = requests.get(url)
    df = pd.DataFrame(json.loads(response.content))
    qtd = len(df.index)
    return qtd


def e_valida(ti):
    qtd = ti.xcom_pull(task_ids="captura_conta_dados")
    if qtd > 1000:
        return "valido"
    return "nvalido"


with DAG(
    "tutorial_dag",
    start_date=datetime(2024, 8, 13),
    schedule_interval="30 * * * * *",
    catchup=False,
) as DAG:

    captura_conta_dados = PythonOperator(
        task_id="captura_conta_dados", python_callable=captura_conta_dados
    )

    e_valida = BranchPythonOperator(task_id="e_valida", python_callable=e_valida)

    valido = BashOperator(task_id="valido", bash_command="echo 'Quantidade Ok")

    nvalido = BashOperator(task_id="nvalido", bash_command="echo 'Quantidade não Ok")

    captura_conta_dados >> e_valida >> [valido, nvalido]
