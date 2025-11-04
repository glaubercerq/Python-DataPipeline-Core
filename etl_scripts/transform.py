"""
Módulo de Transformação (Transform) - Fase T do ETL
Responsável pela limpeza, padronização e enriquecimento dos dados.
Esta é a fase mais importante do pipeline ETL.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Realiza a limpeza básica dos dados.
    
    Ações:
    - Remove duplicatas
    - Trata valores nulos
    - Remove espaços em branco
    
    Args:
        df: DataFrame original
    
    Returns:
        DataFrame limpo
    """
    logger.info("🧹 Iniciando limpeza de dados...")
    
    df_clean = df.copy()
    rows_before = len(df_clean)
    
    # Remove duplicatas
    df_clean = df_clean.drop_duplicates()
    duplicates_removed = rows_before - len(df_clean)
    if duplicates_removed > 0:
        logger.info(f"  - Removidas {duplicates_removed} linhas duplicadas")
    
    # Remove espaços em branco de colunas texto
    text_columns = df_clean.select_dtypes(include=['object']).columns
    for col in text_columns:
        df_clean[col] = df_clean[col].str.strip() if df_clean[col].dtype == 'object' else df_clean[col]
    
    # Registra valores nulos antes do tratamento
    null_counts = df_clean.isnull().sum()
    if null_counts.sum() > 0:
        logger.warning(f"  ⚠️ Valores nulos encontrados:\n{null_counts[null_counts > 0]}")
    
    # Tratamento de valores nulos específico por coluna
    if 'Preco_Local' in df_clean.columns:
        # Remove linhas onde preço é nulo (dado crítico)
        df_clean = df_clean.dropna(subset=['Preco_Local'])
    
    if 'Quantidade' in df_clean.columns:
        # Preenche quantidade nula com 1
        df_clean['Quantidade'] = df_clean['Quantidade'].fillna(1)
    
    if 'Produto' in df_clean.columns:
        # Preenche produto nulo com "Não Identificado"
        df_clean['Produto'] = df_clean['Produto'].fillna('Não Identificado')
    
    logger.info(f"✅ Limpeza concluída: {len(df_clean)} registros válidos")
    return df_clean


def standardize_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza tipos de dados e formatos.
    
    Ações:
    - Converte datas para datetime
    - Padroniza texto (lowercase)
    - Garante tipos numéricos corretos
    
    Args:
        df: DataFrame limpo
    
    Returns:
        DataFrame padronizado
    """
    logger.info("📏 Padronizando dados...")
    
    df_std = df.copy()
    
    # Converte coluna de data
    if 'Data_Venda' in df_std.columns:
        df_std['Data_Venda'] = pd.to_datetime(df_std['Data_Venda'], errors='coerce')
        logger.info("  - Data_Venda convertida para datetime")
    
    # Padroniza texto para lowercase
    text_columns = ['Produto', 'Categoria', 'Regiao']
    for col in text_columns:
        if col in df_std.columns:
            df_std[col] = df_std[col].str.lower()
    
    # Garante tipos numéricos
    numeric_columns = ['Preco_Local', 'Quantidade']
    for col in numeric_columns:
        if col in df_std.columns:
            df_std[col] = pd.to_numeric(df_std[col], errors='coerce')
    
    logger.info("✅ Padronização concluída")
    return df_std


def enrich_data(df: pd.DataFrame, exchange_rate: dict, crypto_info: dict = None) -> pd.DataFrame:
    """
    Enriquece os dados com cálculos e novas features.
    
    Ações:
    - Converte preços para USD usando taxa de câmbio
    - Calcula valor total da venda
    - Adiciona categorias derivadas
    
    Args:
        df: DataFrame padronizado
        exchange_rate: Dicionário com taxa de câmbio
        crypto_info: Informações de criptomoeda (opcional)
    
    Returns:
        DataFrame enriquecido
    """
    logger.info("💎 Enriquecendo dados...")
    
    df_enriched = df.copy()
    
    # Adiciona taxa de câmbio usada
    rate = exchange_rate['rate']
    df_enriched['Taxa_Cambio'] = rate
    
    # Converte Preço_Local para USD
    if 'Preco_Local' in df_enriched.columns:
        df_enriched['Preco_USD'] = df_enriched['Preco_Local'] * rate
        logger.info(f"  - Preços convertidos usando taxa: {rate}")
    
    # Calcula valor total da venda em USD
    if 'Quantidade' in df_enriched.columns and 'Preco_USD' in df_enriched.columns:
        df_enriched['Valor_Total_USD'] = df_enriched['Quantidade'] * df_enriched['Preco_USD']
        logger.info("  - Valor_Total_USD calculado")
    
    # Adiciona informações de tempo
    if 'Data_Venda' in df_enriched.columns:
        df_enriched['Ano'] = df_enriched['Data_Venda'].dt.year
        df_enriched['Mes'] = df_enriched['Data_Venda'].dt.month
        df_enriched['Dia_Semana'] = df_enriched['Data_Venda'].dt.dayofweek
        df_enriched['Nome_Dia'] = df_enriched['Data_Venda'].dt.day_name()
        logger.info("  - Features de tempo extraídas")
    
    # Categoriza vendas por valor
    if 'Valor_Total_USD' in df_enriched.columns:
        df_enriched['Categoria_Valor'] = pd.cut(
            df_enriched['Valor_Total_USD'],
            bins=[0, 50, 200, 500, float('inf')],
            labels=['Baixo', 'Médio', 'Alto', 'Premium']
        )
        logger.info("  - Categoria_Valor criada")
    
    # Adiciona timestamp de processamento
    df_enriched['Data_Processamento'] = datetime.now().isoformat()
    
    logger.info("✅ Enriquecimento concluído")
    return df_enriched


def aggregate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega dados para análise (preparação para BI/DW).
    
    Cria resumo diário de vendas com métricas agregadas.
    
    Args:
        df: DataFrame enriquecido
    
    Returns:
        DataFrame agregado por data
    """
    logger.info("📊 Agregando dados...")
    
    # Agrupa por data
    df_agg = df.groupby('Data_Venda').agg({
        'Valor_Total_USD': ['sum', 'mean', 'count'],
        'Quantidade': 'sum',
        'Preco_USD': 'mean',
        'Produto': 'nunique'  # Número de produtos únicos vendidos no dia
    }).reset_index()
    
    # Renomeia colunas para clareza
    df_agg.columns = [
        'Data_Venda',
        'Total_Vendas_USD',
        'Ticket_Medio_USD',
        'Numero_Transacoes',
        'Quantidade_Total',
        'Preco_Medio_USD',
        'Produtos_Unicos'
    ]
    
    # Adiciona métricas derivadas
    df_agg['Itens_Por_Transacao'] = df_agg['Quantidade_Total'] / df_agg['Numero_Transacoes']
    
    logger.info(f"✅ Agregação concluída: {len(df_agg)} períodos")
    return df_agg


def transform_data(vendas_df: pd.DataFrame, exchange_rate: dict, crypto_info: dict = None) -> tuple:
    """
    Função principal de transformação que orquestra todo o processo.
    
    Args:
        vendas_df: DataFrame de vendas extraído
        exchange_rate: Taxa de câmbio da API
        crypto_info: Informações de criptomoeda (opcional)
    
    Returns:
        Tupla com (df_detalhado, df_agregado)
    """
    logger.info("=" * 60)
    logger.info("🔄 INICIANDO TRANSFORMAÇÃO DE DADOS")
    logger.info("=" * 60)
    
    # Pipeline de transformação
    df = clean_data(vendas_df)
    df = standardize_data(df)
    df = enrich_data(df, exchange_rate, crypto_info)
    
    # Cria versão agregada
    df_aggregated = aggregate_data(df)
    
    logger.info("=" * 60)
    logger.info("✅ TRANSFORMAÇÃO CONCLUÍDA COM SUCESSO")
    logger.info("=" * 60)
    logger.info(f"📌 Dados detalhados: {len(df)} registros")
    logger.info(f"📌 Dados agregados: {len(df_aggregated)} períodos")
    
    return df, df_aggregated


# Para teste standalone
if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DO MÓDULO DE TRANSFORMAÇÃO")
    print("=" * 60)
    
    # Dados de teste
    test_data = {
        'Data_Venda': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'Produto': ['Notebook', 'Mouse', 'Teclado'],
        'Categoria': ['Eletrônicos', 'Acessórios', 'Acessórios'],
        'Quantidade': [1, 2, 1],
        'Preco_Local': [3000.00, 50.00, 150.00],
        'Regiao': ['Sul', 'Norte', 'Sul']
    }
    
    df_test = pd.DataFrame(test_data)
    exchange_test = {'base': 'BRL', 'target': 'USD', 'rate': 0.20, 'date': '2024-01-01'}
    
    df_detailed, df_agg = transform_data(df_test, exchange_test)
    
    print("\n📊 DADOS DETALHADOS:")
    print(df_detailed.head())
    
    print("\n📈 DADOS AGREGADOS:")
    print(df_agg)
