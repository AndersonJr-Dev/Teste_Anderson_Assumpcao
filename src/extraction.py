import os
import requests
from lxml import html
from urllib.parse import urljoin

# Configurações
BASE_URL = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"
OUTPUT_DIR = "data/raw"

def get_links(url):
    """Retorna lista de links limpos de uma URL."""
    print(f"Varrendo: {url}")
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        tree = html.fromstring(response.content)
        links = tree.xpath('//a/@href')
        
        clean_links = []
        for l in links:
            # Ignora navegação e query strings
            if l in ['../', '/', '#'] or l.startswith('?') or l.startswith('/'):
                continue
            clean_links.append(l)
        return clean_links
    except Exception as e:
        print(f"Erro ao acessar {url}: {e}")
        return []

def download_file(url, filename, subdir):
    """Baixa o arquivo para data/raw/SUBDIR/filename"""
    folder_path = os.path.join(OUTPUT_DIR, subdir)
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    filepath = os.path.join(folder_path, filename)
    # Verifica se já existe
    if os.path.exists(filepath):
        print(f"   [!] Arquivo já existe (pulando): {filename}")
        return filepath

    print(f"   [v] Baixando {filename}...")
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"   [ok] Salvo em: {filepath}")
        return filepath
    except Exception as e:
        print(f"   [x] Falha no download: {e}")
        return None

def main():
    # 1. Identificar Anos
    links_raiz = get_links(BASE_URL)
    anos = [l.replace('/', '') for l in links_raiz if l.replace('/', '').isdigit()]
    anos.sort(reverse=True) # Começa pelos anos mais recentes (2025, 2024...)
    
    candidatos = [] # Lista para guardar (Ano, Trimestre, URL_do_ZIP)

    print(f"Anos encontrados: {anos}")
    
    # 2. Para cada ano, identificar trimestres e arquivos ZIP
    for ano in anos[:2]: 
        url_ano = urljoin(BASE_URL, f"{ano}/")
        itens = get_links(url_ano)
        
        # Inverte a lista para tentar pegar os trimestres finais primeiro (4T, 3T...) se estiverem ordenados
        itens.sort(reverse=True) 
        
        for item in itens:
            full_url = urljoin(url_ano, item)
            
            # CASO A: O item JÁ É o arquivo ZIP (Ex: 3T2025.zip)
            if item.lower().endswith('.zip'):
                trimestre = item.split('.')[0] # Pega '3T2025' do nome
                candidatos.append({
                    'ano': ano,
                    'nome_arquivo': item,
                    'identificador': trimestre,
                    'url': full_url
                })
            
            # CASO B: O item É UMA PASTA (Ex: 3T2025/)
            elif item.endswith('/'):
                # Entra na pasta para ver se tem zip dentro
                sub_itens = get_links(full_url)
                for sub in sub_itens:
                    if sub.lower().endswith('.zip'):
                        candidatos.append({
                            'ano': ano,
                            'nome_arquivo': sub,
                            'identificador': item.replace('/', ''),
                            'url': urljoin(full_url, sub)
                        })

    # 3. Selecionar os Top 3
    if not candidatos:
        print("ERRO: Nenhum arquivo ZIP encontrado.")
        return

    # Garante que não pegamos duplicatas e limitamos a 3
    selecionados = candidatos[:3]
    
    print(f"\n--- Baixando os {len(selecionados)} arquivos mais recentes ---")
    
    for c in selecionados:
        print(f"Alvo: {c['identificador']} ({c['nome_arquivo']})")
        # Cria uma pasta com o nome do trimestre para organizar
        subdir = f"{c['ano']}_{c['identificador']}"
        download_file(c['url'], c['nome_arquivo'], subdir)

if __name__ == "__main__":
    main()