from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importando as funções dos arquivos externos
from utils.download_parquet import download_parquet_from_gcs
from utils.manipular_parquet import manipular_parquet
from utils.processar_lotes import processar_lotes

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
    schedule_interval=timedelta(hours=1),
    catchup=False
) as dag:

    baixar_parquet_task = PythonOperator(
        task_id='baixar_parquet',
        python_callable=download_parquet_from_gcs,
        op_kwargs={
            'bucket_name': 'desafio-eng-dados',
            'source_blob_name': '2024-03-06.pq',
            'destination_file_name': '/tmp/2024-03-06.pq'
        }
    )

    manipular_parquet_task = PythonOperator(
        task_id='manipular_parquet',
        python_callable=manipular_parquet,
        op_kwargs={'file_path': '/tmp/2024-03-06.pq'}
    )

    processar_lotes_task = PythonOperator(
        task_id='processar_lotes',
        python_callable=processar_lotes
    )

    baixar_parquet_task >> manipular_parquet_task >> processar_lotes_task
