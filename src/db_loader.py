import sqlite3
import pandas as pd
import os

# Configurações
DB_PATH = "sql/teste_ans.db"
CSV_CONSOLIDADO = "data/consolidado.csv"
CSV_AGREGADO = "data/despesas_agregadas.csv"
# Função para conectar ao banco de dados
def get_connection():
    return sqlite3.connect(DB_PATH)
# Função para criar tabelas e estruturar o banco de dados
def setup_database():
    print("--- Configurando Banco de Dados (SQLite) ---")
    
    # Garante que a pasta sql existe
    os.makedirs("sql", exist_ok=True)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. DDL - Criação das Tabelas
    print("1. Criando tabelas...")
    
    # Tabela Operadoras (Dimensão)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS operadoras (
        reg_ans TEXT PRIMARY KEY,
        cnpj TEXT,
        razao_social TEXT,
        uf TEXT,
        modalidade TEXT
    );
    """)
    
    # Tabela Despesas (Fatos)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS despesas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reg_ans TEXT,
        trimestre TEXT,
        ano INTEGER,
        valor_despesa REAL,
        descricao TEXT,
        FOREIGN KEY (reg_ans) REFERENCES operadoras(reg_ans)
    );
    """)
    
    # Tabela Agregada (Para consultas rápidas/Dashboard)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS despesas_agregadas (
        razao_social TEXT,
        uf TEXT,
        total_despesas REAL,
        media_trimestral REAL,
        desvio_padrao REAL,
        qtd_lancamentos INTEGER
    );
    """)
    
    conn.commit()
    conn.close()
    print("   [v] Tabelas criadas com sucesso.")
# Função para importar dados dos CSVs para o banco de dados
def import_data():
    print("\n--- Iniciando Importação de Dados ---")
    conn = get_connection()
    
    # 1. Importar Operadoras (Normalização)
    if os.path.exists(CSV_CONSOLIDADO):
        print("1. Carregando Operadoras...")
        df = pd.read_csv(CSV_CONSOLIDADO, sep=';', encoding='utf-8')
        
        # Extrai apenas as colunas únicas de operadoras (remove duplicatas)
        df_ops = df[['REG_ANS', 'CNPJ', 'RAZAO_SOCIAL', 'UF', 'MODALIDADE']].drop_duplicates(subset=['REG_ANS'])
        
        # Renomeia colunas para o padrão do banco (snake_case)
        df_ops.columns = ['reg_ans', 'cnpj', 'razao_social', 'uf', 'modalidade']
        
        # Insere no banco
        df_ops.to_sql('operadoras', conn, if_exists='replace', index=False)
        print(f"   [v] {len(df_ops)} operadoras importadas.")
        
        # 2. Importar Despesas
        print("2. Carregando Despesas Detalhadas...")
        df_desp = df[['REG_ANS', 'TRIMESTRE', 'ANO', 'VALOR_DESPESA', 'DESCRICAO']]
        df_desp.columns = ['reg_ans', 'trimestre', 'ano', 'valor_despesa', 'descricao']
        # Insere no banco
        df_desp.to_sql('despesas', conn, if_exists='replace', index=False)
        print(f"   [v] {len(df_desp)} registros de despesas importados.")
        
    else:
        print(f"[ERRO] {CSV_CONSOLIDADO} não encontrado.")

    # 3. Importar Agregados
    if os.path.exists(CSV_AGREGADO):
        print("3. Carregando Dados Agregados...")
        df_agg = pd.read_csv(CSV_AGREGADO, sep=';', encoding='utf-8')
        # Ajusta nomes para minusculo para bater com o banco
        df_agg.columns = [c.lower() for c in df_agg.columns]
        
        df_agg.to_sql('despesas_agregadas', conn, if_exists='replace', index=False)
        print(f"   [v] {len(df_agg)} registros agregados importados.")
    
    conn.close()
    print(f"\nBanco de Dados Populado: {os.path.abspath(DB_PATH)}")

if __name__ == "__main__":
    setup_database()
    import_data()