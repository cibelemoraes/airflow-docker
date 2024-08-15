from google.cloud import bigquery

def carregar_no_bigquery(dados_transformados, dataset_id, table_id):
    client = bigquery.Client()
    table_ref = client.dataset(dataset_id).table(table_id)
    
    errors = client.insert_rows_json(table_ref, dados_transformados)
    if errors:
        print(f"Erros ao inserir no BigQuery: {errors}")
    else:
        print("Dados carregados com sucesso no BigQuery")
