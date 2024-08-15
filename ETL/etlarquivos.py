from google.cloud import storage
import pandas as pd

def download_parquet_from_gcs(bucket_name, source_blob_name, destination_file_name):
    # Inicializa o cliente de storage
    client = storage.Client()

    # Acessa o bucket e o blob (arquivo)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    # Faz o download do arquivo para o caminho especificado
    blob.download_to_filename(destination_file_name)
    print(f"Arquivo {source_blob_name} baixado para {destination_file_name}")

def manipular_parquet(file_path):
    # Lê o arquivo Parquet
    df = pd.read_parquet(file_path)

    # Exemplo de manipulação: exibir as 5 primeiras linhas
    print(df.head())

if __name__ == "__main__":
    # Defina o bucket e o arquivo
    bucket_name = "desafio-eng-dados"
    source_blob_name = "2024-03-06.pq"
    destination_file_name = "/tmp/2024-03-06.pq"  # Caminho temporário

    # Faz o download do arquivo Parquet
    download_parquet_from_gcs(bucket_name, source_blob_name, destination_file_name)

    # Manipula o arquivo Parquet
    manipular_parquet(destination_file_name)
