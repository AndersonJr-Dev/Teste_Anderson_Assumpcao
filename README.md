Teste Técnico - Intuitive Care
Candidato: Anderson Assumpção Junior Vaga: Estágio em Desenvolvimento.

Este repositório contém a solução para o desafio técnico, abrangendo ETL de dados públicos da ANS, validação de dados, modelagem de banco de dados e análise SQL.

🛠 Tecnologias Escolhidas
Linguagem: Python 3.14 (Escolhido pela robustez em manipulação de dados com Pandas e agilidade de desenvolvimento).

Banco de Dados: SQLite (Escolhido pela portabilidade para este teste, dispensando instalação de servidores externos, mas mantendo sintaxe SQL compatível com MySQL/PostgreSQL).

Bibliotecas Principais: pandas, requests, lxml.

🚀 Como Executar o Projeto
O projeto foi construído de forma modular. Siga a ordem abaixo para reproduzir todo o pipeline:

1. Configuração Inicial
Certifique-se de ter o Python instalado e as dependências:

Bash
# Criação do ambiente virtual (recomendado)
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows

# Instalação das dependências
pip install pandas requests lxml
2. Pipeline de Execução
Passo 1: ETL e Consolidação (Item 1) Baixa, extrai, trata encoding e consolida os CSVs trimestrais.

Bash
python src/processor.py
Saída: Gera data/consolidado.csv.

Passo 2: Análise e Agregação (Item 2) Gera estatísticas por operadora/UF e valida matematicamente os CNPJs.

Bash
python src/aggregator.py
Saída: Gera data/despesas_agregadas.csv e exibe relatório de validação no terminal.

Passo 3: Banco de Dados e Carga (Item 3) Cria o banco SQLite, estrutura as tabelas (DDL) e importa os dados (DML).

Bash
python src/db_loader.py
Saída: Cria sql/teste_ans.db e popula as tabelas.

Passo 4: Queries Analíticas (Item 3.4) Executa as queries SQL complexas exigidas no teste (Top 5 Crescimento, Distribuição UF, Consistência).

Bash
python src/analytics_queries.py
⚖️ Diário de Decisões (Trade-offs)
Documentação das escolhas técnicas baseadas nos requisitos do teste.

1. Estratégia de Processamento de Arquivos (ETL)
Decisão: Processamento Incremental (chunksize).


Contexto: O teste questionou entre processar em memória ou incrementalmente.

Justificativa: Optei por ler os CSVs em chunks (lotes) de 5.000 linhas. Embora o volume atual caiba na memória, essa abordagem garante que a aplicação seja escalável e não trave caso a ANS disponibilize arquivos de Gigabytes no futuro.

2. Tratamento de Encoding (Resiliência)
Problema: Os arquivos da ANS misturam encodings (UTF-8 e Latin-1), gerando caracteres quebrados ("Mojibake") como FUNDAÃÃO.

Solução: Implementei uma estratégia híbrida de decodificação. O script tenta ler como utf-8 primeiro (padrão moderno); se falhar, faz fallback para latin1 com tratamento de erro. Isso garantiu 100% de legibilidade nos nomes das operadoras.

3. Validação de Dados
Decisão: Validação Matemática de CNPJ.


Contexto: O teste pedia tratamento para CNPJs inválidos.

Justificativa: Em vez de apenas verificar o tamanho da string (regex), implementei a classe DataValidator que calcula os Dígitos Verificadores (módulo 11). Isso garante que apenas empresas reais sejam processadas, aumentando a qualidade do dado final.

4. Modelagem do Banco de Dados (Normalização)

Decisão: Opção B - Tabelas Normalizadas.

Estrutura: Separei os dados em duas tabelas principais:

operadoras (Dimensão): reg_ans (PK), cnpj, razao_social, uf.

despesas (Fatos): id, reg_ans (FK), valor, trimestre.

Justificativa:

Integridade: Evita que uma mesma operadora tenha nomes diferentes em trimestres diferentes.

Armazenamento: O nome da operadora (string longa) é armazenado apenas uma vez, e não repetido milhões de vezes na tabela de despesas, economizando espaço e processamento.

5. Tipagem de Dados no SQL
Decisão: Uso de DECIMAL/REAL para valores monetários.

Justificativa: Não utilizei FLOAT simples devido a erros de precisão em ponto flutuante. Para sistemas financeiros/contábeis, a precisão decimal é crítica.

📂 Estrutura do Projeto
Plaintext
├── data/                   # Armazenamento de arquivos
│   ├── raw/                # Arquivos brutos baixados (ZIPs)
│   ├── consolidado.csv     # Resultado do ETL
│   └── despesas_agregadas.csv # Resultado da Agregação
├── sql/
│   └── teste_ans.db        # Banco de Dados SQLite
├── src/
│   ├── processor.py        # Script ETL (Extração e Limpeza)
│   ├── validator.py        # Regras de validação (CNPJ)
│   ├── aggregator.py       # Lógica de estatística e agregação
│   ├── db_loader.py        # Script de criação e carga do Banco
│   └── analytics_queries.py # Relatórios SQL automatizados
└── README.md               # Documentação