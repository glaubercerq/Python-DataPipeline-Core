"""
Script de execução do pipeline ETL
Ponto de entrada simplificado para executar o pipeline.
"""

import sys
import os

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importa e executa o pipeline da nova estrutura modular
from src.etl_pipeline.main import main

if __name__ == "__main__":
    print("🚀 Executando Pipeline ETL...")
    print("=" * 70)
    main()
