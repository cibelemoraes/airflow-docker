# airflow-docker
Desafio Técnico Pedra Pagamentos

# Projeto de ETL Automatizado com Apache Airflow, PostgreSQL, Google Cloud Storage e BigQuery

Este projeto implementa um pipeline de ETL (Extract, Transform, Load) automatizado usando Apache Airflow. Ele verifica novos dados no PostgreSQL, aplica transformações e os persiste no BigQuery. Além disso, o pipeline baixa arquivos Parquet de um bucket no Google Cloud Storage para processamento.

## Estrutura do Projeto
dags/
│
├── dag_parquet_e_lotes.py    # Arquivo da DAG principal
├── utils/
│   ├── download_parquet.py   # Arquivo com a função de download do Parquet
│   ├── manipular_parquet.py  # Arquivo com a função de manipulação do Parquet
│   └── processar_lotes.py    # Arquivo com as funções de processamento de lotes
│
├── dag_etl_data_warehouse.py # Arquivo da DAG principal
└── utils2/
├── download_parquet.py      # Função para baixar arquivos Parquet do GCS
├── verificar_novos_dados.py  # Função para verificar novos dados no PostgreSQL
├── processar_dados.py        # Função para transformar/processar os dados
└── carregar_no_bigquery.py  # Função para carregar os dados no BigQuery


## Funcionalidades

1. **Download de arquivos Parquet do Google Cloud Storage**.
2. **Verificação de novos dados no PostgreSQL**.
3. **Processamento e transformação dos dados**.
4. **Persistência dos dados no BigQuery**.
5. **Automação do processo com Airflow, rodando a cada 1 hora**.

## Pré-requisitos

- **Python 3.8+**
- **Apache Airflow 2.x**
- **PostgreSQL**
- **Google Cloud SDK**
- **Bibliotecas Python**: `google-cloud-storage`, `google-cloud-bigquery`, `psycopg2`, `pandas`

## Instalação

### 1. Configuração do Ambiente

1. Clone o repositório:
    ```bash
    git clone https://github.com/seu-usuario/seu-repositorio.git
    cd seu-repositorio
    ```

2. Crie e ative um ambiente virtual:
    ```bash
    python -m venv venv
    source venv/bin/activate  # Linux/Mac
    venv\Scripts\activate  # Windows
    ```

3. Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

4. Instale o Google Cloud SDK e autentique-se:
    ```bash
    gcloud auth application-default login
    ```

### 2. Configuração do Airflow

1. Inicialize o Airflow:
    ```bash
    airflow db init
    ```

2. Crie um usuário admin:
    ```bash
    airflow users create --username admin --password admin --firstname Admin --lastname Admin --role Admin --email admin@example.com
    ```

3. Inicie o Airflow:
    ```bash
    airflow webserver --port 8080
    airflow scheduler
    ```

### 3. Configuração do PostgreSQL

1. Certifique-se de que o PostgreSQL esteja rodando e crie as tabelas necessárias:
    ```sql
    CREATE TABLE tabela_origem (
        id SERIAL PRIMARY KEY,
        nome_lote VARCHAR(255),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE tabela_destino (
        id SERIAL PRIMARY KEY,
        nome_lote VARCHAR(255),
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    ```

2. Insira alguns dados para teste na tabela de origem.

### 4. Configuração do Google Cloud

1. Crie um bucket no Google Cloud Storage e faça upload dos arquivos Parquet.
2. Configure um dataset e uma tabela no BigQuery para armazenar os dados processados.

## Execução

1. No Airflow, a DAG estará disponível na interface web (http://localhost:8080). Habilite a DAG `dag_etl_data_warehouse` para iniciar o processo de ETL automatizado.

## Estrutura do Código

### Arquivo Principal da DAG: `dag_etl_data_warehouse.py`

Este arquivo define a lógica do fluxo de trabalho, conectando as tarefas para baixar o Parquet, verificar novos dados, processá-los e carregá-los no BigQuery.

### Módulos Utilitários

- **`download_parquet.py`**: Função para baixar arquivos Parquet do Google Cloud Storage.
- **`verificar_novos_dados.py`**: Função para consultar novos registros no PostgreSQL.
- **`processar_dados.py`**: Função para transformar e processar os dados.
- **`carregar_no_bigquery.py`**: Função para inserir os dados transformados no BigQuery.

## Customização

- **Conexões PostgreSQL**: Ajuste as configurações de conexão no arquivo `verificar_novos_dados.py`.
- **Google Cloud**: Certifique-se de que as credenciais do Google estão configuradas corretamente para acesso ao BigQuery e ao Google Cloud Storage.

## Solução de Problemas

1. Verifique se o Airflow está rodando corretamente e se as dependências foram instaladas.
2. Confira as credenciais do Google Cloud e o acesso aos buckets e datasets.
3. Monitore os logs das tarefas no Airflow para diagnosticar falhas.

## Contribuições

Sinta-se à vontade para abrir issues ou enviar pull requests.

Esse README fornece uma visão clara de como configurar e rodar o projeto, incluindo detalhes das ferramentas utilizadas e instruções específicas para cada parte do pipeline

