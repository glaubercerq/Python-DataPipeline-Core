"""
Script de Limpeza - Reset do Pipeline
Execute este script para limpar dados gerados e resetar o projeto
"""

import os
import shutil

def print_header(text):
    print("\n" + "=" * 60)
    print(f"  {text}")
    print("=" * 60)

def clean_database():
    """Remove arquivos de banco de dados."""
    print_header("🗑️ Limpando Banco de Dados")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_dir = os.path.join(base_dir, 'data', 'database')
    
    if os.path.exists(db_dir):
        files = os.listdir(db_dir)
        for file in files:
            if file.endswith('.db'):
                file_path = os.path.join(db_dir, file)
                try:
                    os.remove(file_path)
                    print(f"✅ Removido: {file}")
                except Exception as e:
                    print(f"❌ Erro ao remover {file}: {e}")
    else:
        print("ℹ️ Diretório de banco não existe")

def clean_processed():
    """Remove arquivos processados."""
    print_header("🗑️ Limpando Dados Processados")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    processed_dir = os.path.join(base_dir, 'data', 'processed')
    
    if os.path.exists(processed_dir):
        files = os.listdir(processed_dir)
        if files:
            for file in files:
                file_path = os.path.join(processed_dir, file)
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"✅ Removido: {file}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        print(f"✅ Removida pasta: {file}")
                except Exception as e:
                    print(f"❌ Erro ao remover {file}: {e}")
        else:
            print("ℹ️ Nenhum arquivo processado encontrado")
    else:
        print("ℹ️ Diretório de processados não existe")

def clean_logs():
    """Remove arquivos de log."""
    print_header("🗑️ Limpando Logs")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    logs_dir = os.path.join(base_dir, 'logs')
    
    if os.path.exists(logs_dir):
        files = os.listdir(logs_dir)
        removed = False
        for file in files:
            if file.endswith('.log'):
                file_path = os.path.join(logs_dir, file)
                try:
                    os.remove(file_path)
                    print(f"✅ Removido: {file}")
                    removed = True
                except Exception as e:
                    print(f"❌ Erro ao remover {file}: {e}")
        if not removed:
            print("ℹ️ Nenhum arquivo de log encontrado")
    else:
        print("ℹ️ Diretório de logs não existe")

def clean_pycache():
    """Remove diretórios __pycache__."""
    print_header("🗑️ Limpando Cache Python")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    removed_count = 0
    for root, dirs, files in os.walk(base_dir):
        if '__pycache__' in dirs:
            pycache_path = os.path.join(root, '__pycache__')
            try:
                shutil.rmtree(pycache_path)
                print(f"✅ Removido: {pycache_path}")
                removed_count += 1
            except Exception as e:
                print(f"❌ Erro ao remover {pycache_path}: {e}")
    
    if removed_count == 0:
        print("ℹ️ Nenhum __pycache__ encontrado")

def clean_coverage():
    """Remove diretório de cobertura de testes."""
    print_header("🗑️ Limpando Cobertura de Testes")
    
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    coverage_dir = os.path.join(base_dir, 'htmlcov')
    
    if os.path.exists(coverage_dir):
        try:
            shutil.rmtree(coverage_dir)
            print("✅ Removido: htmlcov/")
        except Exception as e:
            print(f"❌ Erro ao remover htmlcov/: {e}")
    else:
        print("ℹ️ Diretório htmlcov não existe")

def main():
    """Função principal."""
    print("\n")
    print("╔══════════════════════════════════════════════════════════╗")
    print("║                                                          ║")
    print("║          🧹 LIMPEZA DO PROJETO ETL 🧹                   ║")
    print("║                                                          ║")
    print("╚══════════════════════════════════════════════════════════╝")
    
    print("\n⚠️ Este script irá remover:")
    print("  - Arquivos de banco de dados (.db)")
    print("  - Dados processados")
    print("  - Arquivos de log")
    print("  - Cache Python (__pycache__)")
    print("  - Cobertura de testes (htmlcov)")
    print("\n⚠️ Os dados originais (CSV) serão MANTIDOS")
    
    response = input("\nDeseja continuar? (s/n): ")
    
    if response.lower() in ['s', 'sim', 'y', 'yes']:
        clean_database()
        clean_processed()
        clean_logs()
        clean_pycache()
        clean_coverage()
        
        print("\n")
        print("=" * 60)
        print("✅ LIMPEZA CONCLUÍDA!")
        print("=" * 60)
        print("\nO projeto foi resetado. Você pode executar o pipeline novamente com:")
        print("  python -m src.etl_pipeline.main")
    else:
        print("\n❌ Limpeza cancelada")

if __name__ == "__main__":
    main()
