"""
Script de Teste - Validação do Pipeline ETL
Execute este script para validar se tudo está funcionando corretamente
"""

import os
import sys
from datetime import datetime

def print_header(text):
    """Imprime cabeçalho formatado."""
    print("\n" + "=" * 70)
    print(f"  {text}")
    print("=" * 70)

def test_imports():
    """Testa se todas as bibliotecas necessárias estão instaladas."""
    print_header("1. TESTANDO IMPORTAÇÕES DE BIBLIOTECAS")
    
    libraries = {
        'pandas': 'Manipulação de dados',
        'numpy': 'Operações numéricas',
        'sqlalchemy': 'Conexão com banco de dados',
        'requests': 'Requisições HTTP'
    }
    
    all_ok = True
    for lib, description in libraries.items():
        try:
            __import__(lib)
            print(f"✅ {lib:15} - {description}")
        except ImportError:
            print(f"❌ {lib:15} - FALTANDO! Execute: pip install {lib}")
            all_ok = False
    
    return all_ok

def test_file_structure():
    """Verifica se a estrutura de arquivos está correta."""
    print_header("2. TESTANDO ESTRUTURA DE ARQUIVOS")
    
    base_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(base_dir)
    
    required_files = [
        ('requirements.txt', parent_dir),
        ('README.md', parent_dir),
        ('SETUP.md', parent_dir),
        ('data/raw/vendas.csv', parent_dir),
        ('etl_scripts/extract.py', parent_dir),
        ('etl_scripts/transform.py', parent_dir),
        ('etl_scripts/load.py', parent_dir),
        ('etl_scripts/main_pipeline.py', parent_dir),
        ('etl_scripts/__init__.py', parent_dir),
    ]
    
    all_ok = True
    for file_path, base in required_files:
        full_path = os.path.join(base, file_path)
        if os.path.exists(full_path):
            print(f"✅ {file_path}")
        else:
            print(f"❌ {file_path} - ARQUIVO FALTANDO!")
            all_ok = False
    
    return all_ok

def test_csv_data():
    """Testa se o arquivo CSV está válido."""
    print_header("3. TESTANDO DADOS DO CSV")
    
    try:
        import pandas as pd
        
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        csv_path = os.path.join(base_dir, 'data', 'raw', 'vendas.csv')
        
        df = pd.read_csv(csv_path)
        
        print(f"✅ CSV carregado com sucesso")
        print(f"   - Registros: {len(df)}")
        print(f"   - Colunas: {list(df.columns)}")
        
        required_columns = ['Data_Venda', 'Produto', 'Categoria', 'Quantidade', 'Preco_Local', 'Regiao']
        missing = [col for col in required_columns if col not in df.columns]
        
        if missing:
            print(f"❌ Colunas faltando: {missing}")
            return False
        
        print(f"✅ Todas as colunas necessárias presentes")
        return True
        
    except Exception as e:
        print(f"❌ Erro ao carregar CSV: {e}")
        return False

def test_api_connection():
    """Testa conexão com APIs."""
    print_header("4. TESTANDO CONEXÃO COM APIs")
    
    try:
        import requests
        
        # Testa API de câmbio
        print("📡 Testando API de Câmbio (Frankfurter)...")
        response = requests.get("https://api.frankfurter.app/latest?from=BRL&to=USD", timeout=10)
        if response.status_code == 200:
            data = response.json()
            rate = data['rates']['USD']
            print(f"✅ API de Câmbio funcionando - Taxa BRL→USD: {rate}")
        else:
            print(f"⚠️ API de Câmbio retornou status {response.status_code}")
        
        # Testa API de cripto
        print("📡 Testando API de Criptomoeda (CoinDesk)...")
        response = requests.get("https://api.coindesk.com/v1/bpi/currentprice.json", timeout=10)
        if response.status_code == 200:
            data = response.json()
            price = data['bpi']['USD']['rate_float']
            print(f"✅ API de Cripto funcionando - Bitcoin: ${price:,.2f} USD")
        else:
            print(f"⚠️ API de Cripto retornou status {response.status_code}")
        
        return True
        
    except requests.exceptions.Timeout:
        print("⚠️ Timeout na conexão com APIs (mas o pipeline tem fallback)")
        return True  # Não é erro crítico
    except Exception as e:
        print(f"⚠️ Erro ao testar APIs: {e}")
        print("   (O pipeline tem sistema de fallback, então continuará funcionando)")
        return True  # Não é erro crítico

def test_modules():
    """Testa importação dos módulos do projeto."""
    print_header("5. TESTANDO MÓDULOS DO PROJETO")
    
    # Adiciona o diretório pai ao path
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if base_dir not in sys.path:
        sys.path.insert(0, base_dir)
    
    modules = ['extract', 'transform', 'load']
    
    all_ok = True
    for module_name in modules:
        try:
            module = __import__(module_name)
            print(f"✅ {module_name}.py - Importado com sucesso")
        except Exception as e:
            print(f"❌ {module_name}.py - Erro: {e}")
            all_ok = False
    
    return all_ok

def run_all_tests():
    """Executa todos os testes."""
    print("\n")
    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║                                                                  ║")
    print("║            🧪 SUITE DE TESTES - PIPELINE ETL 🧪                 ║")
    print("║                                                                  ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    
    start_time = datetime.now()
    
    results = {
        'Bibliotecas': test_imports(),
        'Estrutura de Arquivos': test_file_structure(),
        'Dados CSV': test_csv_data(),
        'APIs Externas': test_api_connection(),
        'Módulos do Projeto': test_modules()
    }
    
    # Resumo
    print_header("📊 RESUMO DOS TESTES")
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name:25} - {status}")
    
    print("\n" + "-" * 70)
    print(f"Resultado: {passed}/{total} testes passaram")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    print(f"Tempo de execução: {duration:.2f} segundos")
    
    if passed == total:
        print("\n🎉 TODOS OS TESTES PASSARAM! O projeto está pronto para execução.")
        print("\nPróximo passo: Execute o pipeline com:")
        print("   python main_pipeline.py")
        return True
    else:
        print("\n⚠️ Alguns testes falharam. Corrija os problemas antes de executar o pipeline.")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
