from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import storage
import psycopg2
import pandas as pd
import time

# Funções para download do Parquet do GCS e manipulação

def download_parquet_from_gcs(bucket_name, source_blob_name, destination_file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"Arquivo {source_blob_name} baixado para {destination_file_name}")

def manipular_parquet(file_path):
    df = pd.read_parquet(file_path)
    print(df.head())  # Exemplo de manipulação do Parquet

# Funções para processar lotes no PostgreSQL

def conectar_bd():
    conn = psycopg2.connect(
        host="seu_host",
        database="seu_banco",
        user="seu_usuario",
        password="sua_senha"
    )
    return conn

def obter_lotes_pendentes():
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute("SELECT id, nome_lote FROM lotes WHERE status = 'PENDENTE'")
    lotes = cursor.fetchall()
    conn.close()
    return lotes

def processar_lotes():
    lotes_pendentes = obter_lotes_pendentes()
    if not lotes_pendentes:
        print("Nenhum lote pendente para processar.")
        return

    for lote_id, nome_lote in lotes_pendentes:
        print(f"Processando lote: {nome_lote}")
        time.sleep(2)
        atualizar_status_lote(lote_id, 'PROCESSADO')

def atualizar_status_lote(lote_id, status):
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute("UPDATE lotes SET status = %s WHERE id = %s", (status, lote_id))
    conn.commit()
    conn.close()

# Configuração da DAG

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_parquet_e_lotes',
    default_args=default_args,
    description='DAG que baixa Parquet do GCS e processa lotes pendentes a cada 1 hora',
    schedule_interval=timedelta(hours=1),  # Executa a cada 1 hora
    catchup=False
) as dag:

    # Task para baixar o arquivo Parquet do Google Cloud Storage
    baixar_parquet_task = PythonOperator(
        task_id='baixar_parquet',
        python_callable=download_parquet_from_gcs,
        op_kwargs={
            'bucket_name': 'desafio-eng-dados',
            'source_blob_name': '2024-03-06.pq',
            'destination_file_name': '/tmp/2024-03-06.pq'
        }
    )

    # Task para manipular o arquivo Parquet
    manipular_parquet_task = PythonOperator(
        task_id='manipular_parquet',
        python_callable=manipular_parquet,
        op_kwargs={'file_path': '/tmp/2024-03-06.pq'}
    )

    # Task para processar os lotes pendentes
    processar_lotes_task = PythonOperator(
        task_id='processar_lotes',
        python_callable=processar_lotes
    )

    # Definição da sequência de execução das tasks
    baixar_parquet_task >> manipular_parquet_task >> processar_lotes_task
