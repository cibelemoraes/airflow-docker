from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importando as funções dos arquivos externos
from utils2.verificar_novos_dados import verificar_novos_dados
from utils2.processar_dados import processar_dados
from utils2.carregar_no_bigquery import carregar_no_bigquery

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
    'dag_etl_data_warehouse',
    default_args=default_args,
    description='DAG que verifica novos dados no PostgreSQL e os carrega no BigQuery a cada 1 hora',
    schedule_interval=timedelta(hours=1),
    catchup=False
) as dag:

    def extrair_dados(ti):
        novos_dados = verificar_novos_dados()
        ti.xcom_push(key='novos_dados', value=novos_dados)

    def transformar_dados(ti):
        novos_dados = ti.xcom_pull(key='novos_dados', task_ids='extrair_dados')
        dados_transformados = processar_dados(novos_dados)
        ti.xcom_push(key='dados_transformados', value=dados_transformados)

    def carregar_dados_no_bigquery(ti):
        dados_transformados = ti.xcom_pull(key='dados_transformados', task_ids='transformar_dados')
        carregar_no_bigquery(dados_transformados, 'seu_dataset_id', 'sua_table_id')

    extrair_dados_task = PythonOperator(
        task_id='extrair_dados',
        python_callable=extrair_dados
    )

    transformar_dados_task = PythonOperator(
        task_id='transformar_dados',
        python_callable=transformar_dados
    )

    carregar_dados_task = PythonOperator(
        task_id='carregar_dados_no_bigquery',
        python_callable=carregar_dados_no_bigquery
    )

    extrair_dados_task >> transformar_dados_task >> carregar_dados_task
