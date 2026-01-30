import os
import zipfile
import pandas as pd
import requests
import shutil
from io import BytesIO, StringIO
from lxml import html
from urllib.parse import urljoin

# Configurações
RAW_DIR = "data/raw"
OUTPUT_FILE = "data/consolidado.csv"
TEMP_DIR = "data/temp_proc"
URL_CADASTRO_DIR = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/"

def obter_link_cadastro():
    print(f"Procurando arquivo atualizado em: {URL_CADASTRO_DIR}")
    try:
        response = requests.get(URL_CADASTRO_DIR, timeout=30)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        links = tree.xpath('//a/@href')
        for link in links:
            if link.lower().endswith('.csv') and 'metadados' not in link.lower():
                return urljoin(URL_CADASTRO_DIR, link)
        return None
    except Exception as e:
        print(f"[Erro ao varrer pasta]: {e}")
        return None

def obter_mapa_operadoras():
    url_csv = obter_link_cadastro()
    if not url_csv: return {}

    print("Baixando dados cadastrais...")
    try:
        response = requests.get(url_csv, timeout=60)
        response.raise_for_status()
        
        # Tratamento de encoding manual
        try:
            conteudo_texto = response.content.decode('utf-8')
        except UnicodeDecodeError:
            conteudo_texto = response.content.decode('latin1', errors='replace')
        
        df = pd.read_csv(
            StringIO(conteudo_texto), 
            sep=';', 
            dtype=str,
            on_bad_lines='skip'
        )
        
        df.columns = [c.strip().upper() for c in df.columns]
        
        # Mapeamento dinâmico das colunas extras (UF e Modalidade)
        col_reg = next((c for c in df.columns if 'REGISTRO' in c and 'DATA' not in c), None)
        col_cnpj = next((c for c in df.columns if 'CNPJ' in c), None)
        col_nome = next((c for c in df.columns if 'RAZAO' in c or 'NOME' in c), None)
        col_uf = next((c for c in df.columns if 'UF' == c or 'ESTADO' in c), None)
        col_mod = next((c for c in df.columns if 'MODALIDADE' in c), None)
        
        print(f"   [v] Colunas extras mapeadas: UF='{col_uf}' | MODALIDADE='{col_mod}'")
        
        mapa = {}
        for _, row in df.iterrows():
            reg = str(row[col_reg]).strip().lstrip('0')
            cnpj_raw = str(row[col_cnpj]).strip()
            cnpj_limpo = ''.join(filter(str.isdigit, cnpj_raw))
            
            mapa[reg] = {
                'CNPJ': cnpj_limpo if cnpj_limpo else cnpj_raw, 
                'RAZAO_SOCIAL': row[col_nome],
                'UF': row[col_uf] if col_uf else 'ND',
                'MODALIDADE': row[col_mod] if col_mod else 'ND'
            }
        
        print(f"   [v] {len(mapa)} operadoras carregadas.")
        return mapa

    except Exception as e:
        print(f"   [ERRO] Falha ao baixar cadastro: {e}")
        return {}
# Normaliza valores monetários    
def normalizar_valor(valor):
    if pd.isna(valor): return 0.0
    val_str = str(valor).strip()
    if val_str.replace('.','').replace('-','').isdigit():
        return float(val_str)
    val_str = val_str.replace('.', '').replace(',', '.')
    try:
        return float(val_str)
    except:
        return 0.0

def processar_dados():
    mapa_operadoras = obter_mapa_operadoras()
    
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)
    os.makedirs(TEMP_DIR)
    
    dados_consolidados = []
    pastas = sorted([p for p in os.listdir(RAW_DIR) if os.path.isdir(os.path.join(RAW_DIR, p))])
    
    print(f"\n--- Iniciando Processamento ETL ---")

    for pasta in pastas:
        print(f"Processando: {pasta}")
        try:
            partes = pasta.split('_')
            ano, tri = partes[0], partes[1] if len(partes)>1 else "N/A"
        except: ano, tri = "Unknown", "Unknown"

        caminho_pasta = os.path.join(RAW_DIR, pasta)
        zip_file = next((f for f in os.listdir(caminho_pasta) if f.lower().endswith('.zip')), None)
        if not zip_file: continue
        
        with zipfile.ZipFile(os.path.join(caminho_pasta, zip_file), 'r') as z:
            z.extractall(TEMP_DIR)
            csvs = [f for f in os.listdir(TEMP_DIR) if f.lower().endswith('.csv')]
            
            for csv_nome in csvs:
                path_csv = os.path.join(TEMP_DIR, csv_nome)
                try:
                    with open(path_csv, 'rb') as f:
                        raw_data = f.read()
                    # Tratamento de encoding manual
                    try:
                        conteudo_csv = raw_data.decode('utf-8')
                    except UnicodeDecodeError:
                        conteudo_csv = raw_data.decode('latin1', errors='replace')
                    
                    chunks = pd.read_csv(StringIO(conteudo_csv), sep=';', chunksize=5000, on_bad_lines='skip')
                    count = 0
                    for chunk in chunks:
                        chunk.columns = [c.strip().upper() for c in chunk.columns]

                        # Filtra linhas com 'EVENTO' ou 'SINISTRO' na descrição
                        filtro = chunk['DESCRICAO'].astype(str).str.upper().str.contains('EVENTO|SINISTRO')
                        df_filtrado = chunk[filtro].copy()
                        if df_filtrado.empty: continue
                        
                        df_filtrado['REG_ANS'] = df_filtrado['REG_ANS'].astype(str).str.strip().str.lstrip('0')
                        
                        # Mapeamentos
                        df_filtrado['CNPJ'] = df_filtrado['REG_ANS'].map(lambda x: mapa_operadoras.get(x, {}).get('CNPJ', 'N/A'))
                        df_filtrado['RAZAO_SOCIAL'] = df_filtrado['REG_ANS'].map(lambda x: mapa_operadoras.get(x, {}).get('RAZAO_SOCIAL', 'N/A'))
                        df_filtrado['UF'] = df_filtrado['REG_ANS'].map(lambda x: mapa_operadoras.get(x, {}).get('UF', 'ND'))
                        df_filtrado['MODALIDADE'] = df_filtrado['REG_ANS'].map(lambda x: mapa_operadoras.get(x, {}).get('MODALIDADE', 'ND'))     
                        df_filtrado['TRIMESTRE'] = tri
                        df_filtrado['ANO'] = ano
                        df_filtrado['VALOR_DESPESA'] = df_filtrado['VL_SALDO_FINAL'].apply(normalizar_valor)
                        df_filtrado['DESCRICAO'] = df_filtrado['DESCRICAO'].astype(str).str.strip()
                        
                        cols = ['CNPJ', 'RAZAO_SOCIAL', 'UF', 'MODALIDADE', 'TRIMESTRE', 'ANO', 'VALOR_DESPESA', 'DESCRICAO']
                        
                        dados_consolidados.append(df_filtrado[cols])
                        count += len(df_filtrado)
                    print(f"   -> {csv_nome}: {count} linhas.")
                except Exception as e: print(f"   [ERRO] {csv_nome}: {e}")
            for f in os.listdir(TEMP_DIR): os.remove(os.path.join(TEMP_DIR, f))

    if dados_consolidados:
        df_final = pd.concat(dados_consolidados, ignore_index=True)
        df_final.to_csv(OUTPUT_FILE, index=False, sep=';', encoding='utf-8')
        print(f"\nSUCESSO! Arquivo gerado: {OUTPUT_FILE}")
        
        sem_cnpj = len(df_final[df_final['CNPJ'] == 'N/A'])
        print(f"[INFO] Linhas sem match de CNPJ: {sem_cnpj} de {len(df_final)}")
    else:
        print("\n[AVISO] Nada encontrado.")

if __name__ == "__main__":
    processar_dados()
    if os.path.exists(TEMP_DIR): shutil.rmtree(TEMP_DIR)