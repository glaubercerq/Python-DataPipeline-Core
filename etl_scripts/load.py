"""
Módulo de Carregamento (Load) - Fase L do ETL
Responsável por carregar os dados transformados no banco de dados (Data Warehouse).
"""

import pandas as pd
from sqlalchemy import create_engine, text, inspect
import os
import logging
from datetime import datetime
from config import DATABASE_URI, DATABASE_TYPE

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_database_connection(db_uri: str = None) -> create_engine:
    """
    Cria conexão com o banco de dados usando SQLAlchemy.
    
    Args:
        db_uri: URI de conexão do banco. Se None, usa a configuração padrão.
    
    Returns:
        Engine do SQLAlchemy
    """
    if db_uri is None:
        db_uri = DATABASE_URI
    
    # Cria engine SQLAlchemy
    if DATABASE_TYPE == 'sqlite':
        # Para SQLite, garante que o diretório existe
        from config import DATABASE_DIR
        os.makedirs(DATABASE_DIR, exist_ok=True)
    
    engine = create_engine(db_uri, echo=False)
    
    logger.info(f"🔗 Conexão criada com o banco: {db_uri}")
    return engine


def load_to_database(df: pd.DataFrame, table_name: str, engine, load_mode: str = 'replace') -> bool:
    """
    Carrega DataFrame no banco de dados usando Pandas to_sql().
    
    Args:
        df: DataFrame a ser carregado
        table_name: Nome da tabela de destino
        engine: Engine SQLAlchemy
        load_mode: 'replace' (substitui) ou 'append' (adiciona)
    
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        logger.info(f"📥 Carregando dados na tabela '{table_name}'...")
        logger.info(f"  - Modo: {load_mode}")
        logger.info(f"  - Registros: {len(df)}")
        
        # Carrega dados usando to_sql do Pandas
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists=load_mode,  # 'replace' ou 'append'
            index=False,
            chunksize=1000  # Carrega em lotes de 1000 registros
        )
        
        logger.info(f"✅ Tabela '{table_name}' carregada com sucesso!")
        return True
    
    except Exception as e:
        logger.error(f"❌ Erro ao carregar tabela '{table_name}': {e}")
        return False


def create_metadata_table(engine) -> bool:
    """
    Cria tabela de metadados para rastrear execuções do pipeline.
    
    Args:
        engine: Engine SQLAlchemy
    
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        # SQL compatível com PostgreSQL e SQLite
        if DATABASE_TYPE == 'postgresql':
            metadata_sql = """
            CREATE TABLE IF NOT EXISTS etl_metadata (
                execution_id SERIAL PRIMARY KEY,
                execution_date TEXT NOT NULL,
                pipeline_status TEXT NOT NULL,
                records_processed INTEGER,
                tables_loaded TEXT,
                execution_time_seconds REAL,
                notes TEXT
            )
            """
        else:  # SQLite
            metadata_sql = """
            CREATE TABLE IF NOT EXISTS etl_metadata (
                execution_id INTEGER PRIMARY KEY AUTOINCREMENT,
                execution_date TEXT NOT NULL,
                pipeline_status TEXT NOT NULL,
                records_processed INTEGER,
                tables_loaded TEXT,
                execution_time_seconds REAL,
                notes TEXT
            )
            """
        
        with engine.connect() as conn:
            conn.execute(text(metadata_sql))
            conn.commit()
        
        logger.info("📋 Tabela de metadados criada/verificada")
        return True
    
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela de metadados: {e}")
        return False


def log_pipeline_execution(engine, status: str, records: int, tables: list, exec_time: float, notes: str = None) -> bool:
    """
    Registra a execução do pipeline na tabela de metadados.
    
    Args:
        engine: Engine SQLAlchemy
        status: Status da execução ('SUCCESS' ou 'FAILED')
        records: Número de registros processados
        tables: Lista de tabelas carregadas
        exec_time: Tempo de execução em segundos
        notes: Notas adicionais
    
    Returns:
        True se sucesso, False caso contrário
    """
    try:
        metadata_insert = text("""
            INSERT INTO etl_metadata 
            (execution_date, pipeline_status, records_processed, tables_loaded, execution_time_seconds, notes)
            VALUES (:exec_date, :status, :records, :tables, :exec_time, :notes)
        """)
        
        with engine.connect() as conn:
            conn.execute(metadata_insert, {
                'exec_date': datetime.now().isoformat(),
                'status': status,
                'records': records,
                'tables': ', '.join(tables),
                'exec_time': exec_time,
                'notes': notes
            })
            conn.commit()
        
        logger.info(f"📝 Execução registrada: {status}")
        return True
    
    except Exception as e:
        logger.error(f"❌ Erro ao registrar execução: {e}")
        return False


def verify_load(engine, table_name: str) -> dict:
    """
    Verifica se os dados foram carregados corretamente.
    
    Args:
        engine: Engine SQLAlchemy
        table_name: Nome da tabela a verificar
    
    Returns:
        Dicionário com estatísticas da tabela
    """
    try:
        with engine.connect() as conn:
            # Conta registros
            count_query = text(f"SELECT COUNT(*) as total FROM {table_name}")
            result = conn.execute(count_query)
            total_rows = result.fetchone()[0]
            
            # Lista colunas
            inspector = inspect(engine)
            columns = [col['name'] for col in inspector.get_columns(table_name)]
        
        stats = {
            'table': table_name,
            'total_rows': total_rows,
            'columns': columns,
            'column_count': len(columns)
        }
        
        logger.info(f"✅ Verificação '{table_name}': {total_rows} registros, {len(columns)} colunas")
        return stats
    
    except Exception as e:
        logger.error(f"❌ Erro ao verificar tabela '{table_name}': {e}")
        return None


def load_all_data(df_detailed: pd.DataFrame, df_aggregated: pd.DataFrame, 
                  load_mode: str = 'replace') -> bool:
    """
    Função principal que carrega todos os dados no banco.
    
    Args:
        df_detailed: DataFrame com dados detalhados
        df_aggregated: DataFrame com dados agregados
        load_mode: Modo de carga ('replace' ou 'append')
    
    Returns:
        True se todas as cargas foram bem-sucedidas
    """
    logger.info("=" * 60)
    logger.info("💾 INICIANDO CARREGAMENTO NO BANCO DE DADOS")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        # Cria conexão
        engine = create_database_connection()
        
        # Cria tabela de metadados
        create_metadata_table(engine)
        
        # Carrega dados detalhados
        success_detailed = load_to_database(
            df_detailed, 
            'vendas_detalhadas', 
            engine, 
            load_mode
        )
        
        # Carrega dados agregados
        success_aggregated = load_to_database(
            df_aggregated, 
            'vendas_agregadas', 
            engine, 
            load_mode
        )
        
        # Verifica cargas
        if success_detailed and success_aggregated:
            logger.info("\n🔍 Verificando cargas...")
            verify_load(engine, 'vendas_detalhadas')
            verify_load(engine, 'vendas_agregadas')
            
            # Registra execução bem-sucedida
            exec_time = (datetime.now() - start_time).total_seconds()
            log_pipeline_execution(
                engine,
                status='SUCCESS',
                records=len(df_detailed),
                tables=['vendas_detalhadas', 'vendas_agregadas'],
                exec_time=exec_time,
                notes=f'Load mode: {load_mode}'
            )
            
            logger.info("=" * 60)
            logger.info("✅ CARREGAMENTO CONCLUÍDO COM SUCESSO")
            logger.info("=" * 60)
            return True
        else:
            raise Exception("Falha ao carregar uma ou mais tabelas")
    
    except Exception as e:
        logger.error(f"❌ Erro no carregamento: {e}")
        
        # Registra falha
        try:
            exec_time = (datetime.now() - start_time).total_seconds()
            log_pipeline_execution(
                engine,
                status='FAILED',
                records=0,
                tables=[],
                exec_time=exec_time,
                notes=str(e)
            )
        except:
            pass
        
        return False


# Para teste standalone
if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DO MÓDULO DE CARREGAMENTO")
    print("=" * 60)
    
    # Dados de teste
    test_data = pd.DataFrame({
        'Data_Venda': ['2024-01-01', '2024-01-02'],
        'Produto': ['Notebook', 'Mouse'],
        'Valor_Total_USD': [600.0, 10.0]
    })
    
    test_agg = pd.DataFrame({
        'Data_Venda': ['2024-01-01'],
        'Total_Vendas_USD': [610.0],
        'Numero_Transacoes': [2]
    })
    
    success = load_all_data(test_data, test_agg, load_mode='replace')
    
    if success:
        print("\n✅ Teste concluído com sucesso!")
    else:
        print("\n❌ Teste falhou!")
