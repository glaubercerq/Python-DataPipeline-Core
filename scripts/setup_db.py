"""
Script para configurar o banco de dados
Cria as tabelas necessárias e estrutura inicial.
"""

import sys
import os

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl_pipeline.load.load import create_database_connection, create_metadata_table

def setup_database():
    """Configura o banco de dados criando as tabelas necessárias."""
    print("=" * 70)
    print("📊 CONFIGURAÇÃO DO BANCO DE DADOS")
    print("=" * 70)
    
    try:
        # Cria conexão
        engine = create_database_connection()
        
        # Cria tabela de metadados
        create_metadata_table(engine)
        
        print("\n✅ Banco de dados configurado com sucesso!")
        print("   - Tabela etl_metadata criada")
        
        return True
    
    except Exception as e:
        print(f"\n❌ Erro ao configurar banco: {e}")
        return False

if __name__ == "__main__":
    success = setup_database()
    sys.exit(0 if success else 1)
