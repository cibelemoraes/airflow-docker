import psycopg2
import time

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
