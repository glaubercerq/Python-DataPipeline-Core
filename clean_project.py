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
    etl_dir = os.path.join(base_dir, 'etl_scripts')
    
    log_files = ['etl_pipeline.log']
    
    for log_file in log_files:
        log_path = os.path.join(etl_dir, log_file)
        if os.path.exists(log_path):
            try:
                os.remove(log_path)
                print(f"✅ Removido: {log_file}")
            except Exception as e:
                print(f"❌ Erro ao remover {log_file}: {e}")
        else:
            print(f"ℹ️ {log_file} não existe")

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
    print("\n⚠️ Os dados originais (CSV) serão MANTIDOS")
    
    response = input("\nDeseja continuar? (s/n): ")
    
    if response.lower() in ['s', 'sim', 'y', 'yes']:
        clean_database()
        clean_processed()
        clean_logs()
        clean_pycache()
        
        print("\n")
        print("=" * 60)
        print("✅ LIMPEZA CONCLUÍDA!")
        print("=" * 60)
        print("\nO projeto foi resetado. Você pode executar o pipeline novamente com:")
        print("  cd etl_scripts")
        print("  python main_pipeline.py")
    else:
        print("\n❌ Limpeza cancelada")

if __name__ == "__main__":
    main()
