import psycopg2
from psycopg2 import sql
import time

# Função para conectar ao banco de dados PostgreSQL
def conectar_bd():
    conn = psycopg2.connect(
        host="seu_host",
        database="seu_banco",
        user="seu_usuario",
        password="sua_senha"
    )
    return conn

# Inicializar a tabela de controle de lotes
def inicializar_tabela():
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS lotes (
        id SERIAL PRIMARY KEY,
        nome_lote VARCHAR(255) NOT NULL,
        status VARCHAR(50) NOT NULL DEFAULT 'PENDENTE'
    )
    ''')
    conn.commit()
    conn.close()

# Adicionar novos lotes à tabela de controle
def adicionar_lotes(lotes):
    conn = conectar_bd()
    cursor = conn.cursor()
    for lote in lotes:
        cursor.execute("INSERT INTO lotes (nome_lote) VALUES (%s)", (lote,))
    conn.commit()
    conn.close()

# Obter os lotes que ainda não foram processados
def obter_lotes_pendentes():
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute("SELECT id, nome_lote FROM lotes WHERE status = 'PENDENTE'")
    lotes = cursor.fetchall()
    conn.close()
    return lotes

# Processar os lotes pendentes
def processar_lotes():
    lotes_pendentes = obter_lotes_pendentes()
    if not lotes_pendentes:
        print("Nenhum lote pendente para processar.")
        return

    for lote_id, nome_lote in lotes_pendentes:
        print(f"Processando lote: {nome_lote}")
        # Simula o processamento (substitua esta parte pelo processamento real)
        time.sleep(2)
        
        # Atualiza o status do lote para 'PROCESSADO'
        atualizar_status_lote(lote_id, 'PROCESSADO')

    print("Todos os lotes pendentes foram processados.")

# Atualizar o status do lote no banco de dados
def atualizar_status_lote(lote_id, status):
    conn = conectar_bd()
    cursor = conn.cursor()
    cursor.execute("UPDATE lotes SET status = %s WHERE id = %s", (status, lote_id))
    conn.commit()
    conn.close()

# Executar o script
if __name__ == "__main__":
    inicializar_tabela()

    # Exemplo de como adicionar novos lotes
    lotes_novos = ['Lote1', 'Lote2', 'Lote3']
    adicionar_lotes(lotes_novos)

    # Processar os lotes pendentes
    processar_lotes()
