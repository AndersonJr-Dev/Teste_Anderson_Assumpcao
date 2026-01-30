import sqlite3
import pandas as pd

DB_PATH = "sql/teste_ans.db"

def run_queries():
    conn = sqlite3.connect(DB_PATH)
    
    print("--- RELATÓRIO ANALÍTICO SQL (Item 3.4) ---\n")

    # ---------------------------------------------------------
    # QUERY 1: Top 5 Operadoras com Maior Crescimento (%)
    # Lógica: Comparar Soma do 1T2025 vs 3T2025
    # Desafio PDF: "Considerar operadoras que não tem dados em todos trimestres" -> Inner Join filtra isso.
    # ---------------------------------------------------------
    print("1. Top 5 Operadoras com Maior Crescimento (1T2025 vs 3T2025):")
    query1 = """
    WITH despesas_trimestrais AS (
        -- Agrupa despesas por operadora e trimestre
        SELECT reg_ans, trimestre, SUM(valor_despesa) as total
        FROM despesas
        GROUP BY reg_ans, trimestre
    ),
    inicio AS (
        SELECT * FROM despesas_trimestrais WHERE trimestre = '1T2025'
    ),
    fim AS (
        SELECT * FROM despesas_trimestrais WHERE trimestre = '3T2025'
    )
    SELECT 
        op.razao_social,
        inicio.total as valor_inicial,
        fim.total as valor_final,
        ROUND(((fim.total - inicio.total) * 1.0 / inicio.total) * 100, 2) as crescimento_pct
    FROM inicio
    JOIN fim ON inicio.reg_ans = fim.reg_ans
    JOIN operadoras op ON inicio.reg_ans = op.reg_ans
    WHERE inicio.total > 1000 -- Filtro para evitar distorções com valores irrisórios (ex: cresceu de R$1 pra R$10)
    ORDER BY crescimento_pct DESC
    LIMIT 5;
    """
    df1 = pd.read_sql_query(query1, conn)
    print(df1.to_string(index=False))
    print("-" * 50)

    # ---------------------------------------------------------
    # QUERY 2: Distribuição de Despesas por UF (Top 5)
    # Desafio PDF: Calcular também a média por operadora na mesma query
    # ---------------------------------------------------------
    print("\n2. Top 5 Estados com Maiores Despesas (Total + Média por Operadora):")
    query2 = """
    SELECT 
        op.uf,
        SUM(d.valor_despesa) as total_despesas,
        AVG(d.valor_despesa) as media_por_lancamento,
        -- Cálculo da média por operadora (Total do UF / Qtd Operadoras Únicas no UF)
        SUM(d.valor_despesa) / COUNT(DISTINCT op.reg_ans) as media_por_operadora
    FROM despesas d
    JOIN operadoras op ON d.reg_ans = op.reg_ans
    WHERE op.uf != 'ND'
    GROUP BY op.uf
    ORDER BY total_despesas DESC
    LIMIT 5;
    """
    df2 = pd.read_sql_query(query2, conn)
    # Formatação visual
    df2['total_despesas'] = df2['total_despesas'].apply(lambda x: f"R$ {x:,.2f}")
    df2['media_por_operadora'] = df2['media_por_operadora'].apply(lambda x: f"R$ {x:,.2f}")
    print(df2[['uf', 'total_despesas', 'media_por_operadora']].to_string(index=False))
    print("-" * 50)

    # ---------------------------------------------------------
    # QUERY 3: Operadoras acima da média em >= 2 trimestres
    # Trade-off: Usa CTEs para clareza e manutenibilidade.
    # ---------------------------------------------------------
    print("\n3. Contagem de Operadoras com Performance Acima da Média (>= 2 Trimestres):")
    query3 = """
    WITH totais_operadora_trimestre AS (
        -- Quanto cada operadora gastou por trimestre
        SELECT reg_ans, trimestre, SUM(valor_despesa) as total_op
        FROM despesas
        GROUP BY reg_ans, trimestre
    ),
    media_mercado_trimestre AS (
        -- Qual foi a média do mercado naquele trimestre
        SELECT trimestre, AVG(total_op) as media_mercado
        FROM totais_operadora_trimestre
        GROUP BY trimestre
    ),
    operadoras_acima AS (
        -- Filtra quem ficou acima da média
        SELECT t.reg_ans
        FROM totais_operadora_trimestre t
        JOIN media_mercado_trimestre m ON t.trimestre = m.trimestre
        WHERE t.total_op > m.media_mercado
    )
    -- Conta quantas vezes cada operadora apareceu acima da média e filtra >= 2
    SELECT COUNT(*) as qtd_operadoras_consistentes
    FROM (
        SELECT reg_ans
        FROM operadoras_acima
        GROUP BY reg_ans
        HAVING COUNT(*) >= 2
    );
    """
    df3 = pd.read_sql_query(query3, conn)
    print(f"Resultado: {df3.iloc[0,0]} operadoras.")
    print("-" * 50)

    conn.close()

if __name__ == "__main__":
    run_queries()