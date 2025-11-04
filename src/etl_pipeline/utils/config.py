"""
Arquivo de Configuração do Pipeline ETL
Centralize todas as configurações aqui para facilitar manutenção
"""

import os
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# ============================================================
# CONFIGURAÇÕES DE CAMINHOS
# ============================================================

# Diretório base do projeto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Caminhos de dados
DATA_DIR = os.path.join(BASE_DIR, 'data')
RAW_DATA_DIR = os.path.join(DATA_DIR, 'raw')
PROCESSED_DATA_DIR = os.path.join(DATA_DIR, 'processed')
DATABASE_DIR = os.path.join(DATA_DIR, 'database')

# Arquivo de vendas
SALES_CSV_FILE = os.path.join(RAW_DATA_DIR, 'vendas.csv')

# ============================================================
# CONFIGURAÇÕES DE BANCO DE DADOS
# ============================================================

# Tipo de banco de dados (sqlite ou postgresql)
DATABASE_TYPE = os.getenv('DATABASE_TYPE', 'sqlite')

# Configurações SQLite (padrão)
DATABASE_FILE = os.path.join(DATABASE_DIR, 'sales_datawarehouse.db')
SQLITE_URI = f'sqlite:///{DATABASE_FILE}'

# Configurações PostgreSQL
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'sales_datawarehouse')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'etl_user')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'etl_password')
POSTGRES_URI = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}'

# URI do banco de dados baseada no tipo
DATABASE_URI = POSTGRES_URI if DATABASE_TYPE == 'postgresql' else SQLITE_URI

# ============================================================
# CONFIGURAÇÕES DE APIs
# ============================================================

# API de Câmbio (Frankfurter - Gratuita)
EXCHANGE_RATE_API_URL = "https://api.frankfurter.app/latest"
BASE_CURRENCY = 'BRL'
TARGET_CURRENCY = 'USD'

# API de Criptomoeda (CoinDesk - Gratuita)
CRYPTO_API_URL = "https://api.coindesk.com/v1/bpi/currentprice.json"
CRYPTO_SYMBOL = 'BTC'

# Timeout para requisições HTTP (segundos)
API_TIMEOUT = 10

# ============================================================
# CONFIGURAÇÕES DE BANCO DE DADOS
# ============================================================

# Nomes das tabelas
TABLE_DETAILED = 'vendas_detalhadas'
TABLE_AGGREGATED = 'vendas_agregadas'
TABLE_METADATA = 'etl_metadata'

# Modo de carga: 'replace' ou 'append'
LOAD_MODE = 'replace'

# Tamanho do chunk para inserção em lote
CHUNK_SIZE = 1000

# ============================================================
# CONFIGURAÇÕES DE LOGGING
# ============================================================

LOG_LEVEL = 'INFO'  # DEBUG, INFO, WARNING, ERROR, CRITICAL
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = 'etl_pipeline.log'

# ============================================================
# CONFIGURAÇÕES DE TRANSFORMAÇÃO
# ============================================================

# Categorias de valor (bins para pd.cut)
VALUE_CATEGORY_BINS = [0, 50, 200, 500, float('inf')]
VALUE_CATEGORY_LABELS = ['Baixo', 'Médio', 'Alto', 'Premium']

# Colunas que devem ser convertidas para lowercase
TEXT_NORMALIZE_COLUMNS = ['Produto', 'Categoria', 'Regiao']

# Colunas numéricas para validação
NUMERIC_COLUMNS = ['Preco_Local', 'Quantidade']

# ============================================================
# CONFIGURAÇÕES DE VALIDAÇÃO
# ============================================================

# Colunas obrigatórias no CSV
REQUIRED_CSV_COLUMNS = [
    'Data_Venda',
    'Produto',
    'Categoria',
    'Quantidade',
    'Preco_Local',
    'Regiao'
]

# Valores mínimos aceitos
MIN_PRICE = 0.01
MIN_QUANTITY = 1

# ============================================================
# CONFIGURAÇÕES DE FALLBACK
# ============================================================

# Taxa de câmbio padrão caso API falhe
FALLBACK_EXCHANGE_RATE = 0.20  # 1 BRL ≈ 0.20 USD

# Preço de cripto padrão caso API falhe
FALLBACK_CRYPTO_PRICE = 50000.0  # USD

# ============================================================
# CONFIGURAÇÕES DE AMBIENTE
# ============================================================

# Ambiente de execução
ENVIRONMENT = 'development'  # development, production

# Debug mode
DEBUG_MODE = True

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def get_database_connection_string():
    """Retorna string de conexão do banco de dados."""
    return DATABASE_URI

def ensure_directories_exist():
    """Cria diretórios necessários se não existirem."""
    directories = [
        RAW_DATA_DIR,
        PROCESSED_DATA_DIR,
        DATABASE_DIR
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def print_config():
    """Imprime configurações atuais (útil para debug)."""
    print("=" * 60)
    print("CONFIGURAÇÕES DO PIPELINE ETL")
    print("=" * 60)
    print(f"Base Directory: {BASE_DIR}")
    print(f"Database: {DATABASE_FILE}")
    print(f"Sales CSV: {SALES_CSV_FILE}")
    print(f"Exchange Rate: {BASE_CURRENCY} → {TARGET_CURRENCY}")
    print(f"Load Mode: {LOAD_MODE}")
    print(f"Environment: {ENVIRONMENT}")
    print("=" * 60)

# ============================================================
# VALIDAÇÃO DE CONFIGURAÇÃO
# ============================================================

if __name__ == "__main__":
    print_config()
    ensure_directories_exist()
    print("\n✅ Configuração validada e diretórios criados!")
