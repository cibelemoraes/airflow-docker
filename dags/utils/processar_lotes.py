import psycopg2
import csv
import os


# fazendo a conexão com o banco de dados postgre
def conectar_bd():
    conn = psycopg2.connect(
        host="localhost", database="pedrapagamentos", user="admin", password="admin"
    )
    return conn


# Função para importar o arquivo CSV para o banco de dados
def importar_csv_para_bd(caminho_csv):
    if not os.path.exists(caminho_csv):
        print(f"Arquivo CSV não encontrado: {caminho_csv}")
        return

    conn = conectar_bd()
    cursor = conn.cursor()

    try:
        with open(caminho_csv, mode="r", encoding="utf-8") as file:
            leitor_csv = csv.reader(file)
            next(leitor_csv)  # Ignora o cabeçalho do CSV

            insert_query = """
                INSERT INTO lotes (order_number, terminal_id, terminal_serial_number, terminal_model,
                                   terminal_type, provider, technician_email, customer_phone,
                                   customer_id, city, country, country_state, zip_code,
                                   street_name, neighborhood, complement, arrival_date,
                                   deadline_date, cancellation_reason, last_modified_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            for linha in leitor_csv:
                cursor.execute(insert_query, linha)

        conn.commit()
        print(f"Dados do arquivo {caminho_csv} inseridos com sucesso.")
    except Exception as e:
        print(f"Erro ao inserir os dados: {e}")
    finally:
        cursor.close()
        conn.close()


# código percorre automaticamente todos os arquivos CSV na pasta especificada
def processar_arquivos_na_pasta(pasta_origem="D:\\temp"):
    arquivos_csv = [
        os.path.join(pasta_origem, f)
        for f in os.listdir(pasta_origem)
        if f.endswith(".csv")
    ]
    for caminho_csv in arquivos_csv:
        importar_csv_para_bd(caminho_csv)


if __name__ == "__main__":
    processar_arquivos_na_pasta()
