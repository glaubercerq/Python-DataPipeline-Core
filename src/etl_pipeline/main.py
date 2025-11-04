"""
Pipeline Principal ETL - Orquestração do Processo Completo
Este é o script principal que coordena todas as fases: Extract → Transform → Load
"""

from .utils.logger import logger
import sys
from datetime import datetime
import traceback
from pathlib import Path
import os

# Adiciona o diretório raiz ao PYTHONPATH
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

# Importa módulos do projeto
from src.etl_pipeline.extract.extract import extract_all_sources
from src.etl_pipeline.transform.transform import transform_data
from src.etl_pipeline.load.load import load_all_data

# Garante que o diretório de logs existe
os.makedirs(root_dir / 'logs', exist_ok=True)


def print_banner():
    """Imprime banner inicial do pipeline."""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                                                              ║
    ║             🚀 PIPELINE ETL - DATA ENGINEERING 🚀           ║
    ║                                                              ║
    ║  Extract → Transform → Load                                  ║
    ║  Vendas Multimoeda com Integração de APIs                   ║
    ║                                                              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)
    logger.info("Pipeline ETL iniciado")


def print_summary(execution_time: float, records_detailed: int, records_aggregated: int):
    """
    Imprime resumo da execução.
    
    Args:
        execution_time: Tempo total de execução em segundos
        records_detailed: Número de registros detalhados processados
        records_aggregated: Número de registros agregados
    """
    summary = f"""
    ╔══════════════════════════════════════════════════════════════╗
    ║                    📊 RESUMO DA EXECUÇÃO                    ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Status: ✅ SUCESSO                                         ║
    ║  Tempo de Execução: {execution_time:.2f} segundos                      ║
    ║  Registros Detalhados: {records_detailed}                               ║
    ║  Registros Agregados: {records_aggregated}                                ║
    ║  Tabelas Criadas: vendas_detalhadas, vendas_agregadas       ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(summary)


def run_pipeline(load_mode: str = 'replace'):
    """
    Função principal que executa todo o pipeline ETL.
    
    Args:
        load_mode: 'replace' (substitui dados) ou 'append' (adiciona dados)
    
    Returns:
        True se pipeline executou com sucesso, False caso contrário
    """
    start_time = datetime.now()
    
    try:
        print_banner()
        
        # ============================================================
        # FASE 1: EXTRAÇÃO (Extract)
        # ============================================================
        logger.info("")
        logger.info("=" * 70)
        logger.info("FASE 1/3: EXTRAÇÃO DE DADOS")
        logger.info("=" * 70)
        
        vendas_df, exchange_rate, crypto_info = extract_all_sources()
        
        logger.info(f"✅ Extração concluída:")
        logger.info(f"   - Vendas: {len(vendas_df)} registros")
        logger.info(f"   - Taxa de Câmbio: 1 {exchange_rate['base']} = {exchange_rate['rate']} {exchange_rate['target']}")
        logger.info(f"   - Bitcoin: ${crypto_info['usd_price']:,.2f} USD")
        
        # ============================================================
        # FASE 2: TRANSFORMAÇÃO (Transform)
        # ============================================================
        logger.info("")
        logger.info("=" * 70)
        logger.info("FASE 2/3: TRANSFORMAÇÃO DE DADOS")
        logger.info("=" * 70)
        
        df_detailed, df_aggregated = transform_data(
            vendas_df, 
            exchange_rate, 
            crypto_info
        )
        
        logger.info(f"✅ Transformação concluída:")
        logger.info(f"   - Dados detalhados: {len(df_detailed)} registros")
        logger.info(f"   - Dados agregados: {len(df_aggregated)} períodos")
        
        # ============================================================
        # FASE 3: CARREGAMENTO (Load)
        # ============================================================
        logger.info("")
        logger.info("=" * 70)
        logger.info("FASE 3/3: CARREGAMENTO NO BANCO DE DADOS")
        logger.info("=" * 70)
        
        success = load_all_data(df_detailed, df_aggregated, load_mode=load_mode)
        
        if not success:
            raise Exception("Falha no carregamento dos dados")
        
        # ============================================================
        # SUCESSO - Resumo Final
        # ============================================================
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        logger.info("")
        print_summary(execution_time, len(df_detailed), len(df_aggregated))
        
        logger.info("🎉 Pipeline ETL concluído com sucesso!")
        return True
    
    except FileNotFoundError as e:
        logger.error("")
        logger.error("=" * 70)
        logger.error("❌ ERRO: Arquivo não encontrado")
        logger.error("=" * 70)
        logger.error(f"Detalhes: {e}")
        logger.error("Solução: Verifique se o arquivo 'data/raw/vendas.csv' existe")
        return False
    
    except Exception as e:
        logger.error("")
        logger.error("=" * 70)
        logger.error("❌ ERRO NO PIPELINE")
        logger.error("=" * 70)
        logger.error(f"Tipo de erro: {type(e).__name__}")
        logger.error(f"Mensagem: {str(e)}")
        logger.error("")
        logger.error("Stack Trace:")
        logger.error(traceback.format_exc())
        
        return False
    
    finally:
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        logger.info(f"⏱️ Tempo total de execução: {total_time:.2f} segundos")


def main():
    """
    Ponto de entrada principal do programa.
    """
    # Configurações
    LOAD_MODE = 'replace'  # Pode ser 'replace' ou 'append'
    
    # Executa pipeline
    success = run_pipeline(load_mode=LOAD_MODE)
    
    # Retorna código de saída apropriado
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
