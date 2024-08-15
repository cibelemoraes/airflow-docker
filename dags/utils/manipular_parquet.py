import pandas as pd

def manipular_parquet(file_path):
    df = pd.read_parquet(file_path)
    print(df.head())  # Exemplo de manipulação do Parquet
