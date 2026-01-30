import pandas as pd
import os

INPUT_FILE = "data/consolidado.csv"
OUTPUT_FILE = "data/despesas_agregadas.csv"

def gerar_agregacao():
    print("--- Iniciando Agregação de Despesas (Item 2.3) ---")
    
    if not os.path.exists(INPUT_FILE):
        print(f"[ERRO] Arquivo {INPUT_FILE} não encontrado. Rode o processor.py primeiro.")
        return

    # Lê o consolidado.csv
    df = pd.read_csv(INPUT_FILE, sep=';', encoding='utf-8')
    
    # Validação simples - remove linhas sem RAZAO_SOCIAL
    df = df[df['RAZAO_SOCIAL'].notna() & (df['RAZAO_SOCIAL'] != '')]

    print("Calculando estatísticas por Operadora/UF...")
    
    # Agrupa por Razão Social e UF
    # Calcula: Total, Média Trimestral, Desvio Padrão
    agregado = df.groupby(['RAZAO_SOCIAL', 'UF'])['VALOR_DESPESA'].agg(
        TOTAL_DESPESAS='sum',
        MEDIA_TRIMESTRAL='mean',
        DESVIO_PADRAO='std', # Item 2.3 pede desvio padrão
        QTD_LANCAMENTOS='count'
    ).reset_index()

    # Preenche NaN no desvio padrão (caso de registro único) com 0
    agregado['DESVIO_PADRAO'] = agregado['DESVIO_PADRAO'].fillna(0)

    # Ordenação: Maior valor total para menor
    agregado = agregado.sort_values(by='TOTAL_DESPESAS', ascending=False)
    
    # Formatação (arredondar para 2 casas)
    cols_float = ['TOTAL_DESPESAS', 'MEDIA_TRIMESTRAL', 'DESVIO_PADRAO']
    agregado[cols_float] = agregado[cols_float].round(2)

    # Salva o arquivo
    agregado.to_csv(OUTPUT_FILE, index=False, sep=';', encoding='utf-8')
    
    print(f"SUCESSO! Arquivo gerado: {OUTPUT_FILE}")
    print("Top 5 Maiores Despesas:")
    print(agregado.head())

if __name__ == "__main__":
    gerar_agregacao()