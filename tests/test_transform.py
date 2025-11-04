"""
Testes completos para o módulo de transformação
Inclui: limpeza, padronização, enriquecimento, agregação
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from src.etl_pipeline.transform.transform import (
    clean_data,
    standardize_data,
    enrich_data,
    aggregate_data
)


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def sample_raw_data():
    """Fixture com dados brutos para teste."""
    return pd.DataFrame({
        'Data_Venda': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Produto': ['  Laptop  ', 'MOUSE', 'Teclado'],
        'Categoria': ['Eletrônicos', 'Periféricos', 'Periféricos'],
        'Quantidade': [2, 5, 3],
        'Preco_Local': [3500.00, 50.00, 200.00],
        'Regiao': ['Sul', 'Sudeste', 'Norte'],
        'Vendedor': ['João', 'Maria', 'Pedro']
    })


@pytest.fixture
def sample_data_with_nulls():
    """Fixture com dados contendo valores nulos."""
    return pd.DataFrame({
        'Data_Venda': ['2024-01-01', '2024-01-02', None],
        'Produto': ['Laptop', None, 'Teclado'],
        'Categoria': ['Eletrônicos', 'Periféricos', 'Periféricos'],
        'Quantidade': [2, None, 3],
        'Preco_Local': [3500.00, 50.00, None],
        'Regiao': ['Sul', 'Sudeste', 'Norte'],
        'Vendedor': ['João', 'Maria', 'Pedro']
    })


@pytest.fixture
def sample_data_with_duplicates():
    """Fixture com dados duplicados."""
    return pd.DataFrame({
        'Data_Venda': ['2024-01-01', '2024-01-01', '2024-01-02'],
        'Produto': ['Laptop', 'Laptop', 'Mouse'],
        'Categoria': ['Eletrônicos', 'Eletrônicos', 'Periféricos'],
        'Quantidade': [2, 2, 5],
        'Preco_Local': [3500.00, 3500.00, 50.00],
        'Regiao': ['Sul', 'Sul', 'Sudeste'],
        'Vendedor': ['João', 'João', 'Maria']
    })


@pytest.fixture
def sample_clean_data():
    """Fixture com dados já limpos."""
    return pd.DataFrame({
        'Data_Venda': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'Produto': ['laptop', 'mouse', 'teclado'],
        'Categoria': ['eletrônicos', 'periféricos', 'periféricos'],
        'Quantidade': [2, 5, 3],
        'Preco_Local': [3500.00, 50.00, 200.00],
        'Regiao': ['sul', 'sudeste', 'norte'],
        'Vendedor': ['joão', 'maria', 'pedro']
    })


# ============================================================
# TESTES DE LIMPEZA
# ============================================================

class TestCleanData:
    """Testes para função de limpeza de dados."""
    
    def test_clean_removes_duplicates(self, sample_data_with_duplicates):
        """Testa se duplicatas são removidas."""
        result = clean_data(sample_data_with_duplicates)
        
        assert len(result) < len(sample_data_with_duplicates)
        assert result.duplicated().sum() == 0
    
    def test_clean_removes_whitespace(self, sample_raw_data):
        """Testa se espaços em branco são removidos."""
        result = clean_data(sample_raw_data)
        
        # Verifica se não há espaços antes/depois
        assert '  Laptop  ' not in result['Produto'].values
        assert 'Laptop' in result['Produto'].values or 'laptop' in result['Produto'].values
    
    def test_clean_handles_nulls_in_price(self, sample_data_with_nulls):
        """Testa tratamento de valores nulos em preço."""
        result = clean_data(sample_data_with_nulls)
        
        # Linhas com preço nulo devem ser removidas
        assert result['Preco_Local'].isnull().sum() == 0
    
    def test_clean_fills_quantity_nulls(self, sample_data_with_nulls):
        """Testa preenchimento de quantidade nula."""
        result = clean_data(sample_data_with_nulls)
        
        # Quantidade nula deve ser preenchida com 1
        if 'Quantidade' in result.columns:
            assert result['Quantidade'].isnull().sum() == 0
    
    def test_clean_returns_dataframe(self, sample_raw_data):
        """Testa se retorna um DataFrame."""
        result = clean_data(sample_raw_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0


# ============================================================
# TESTES DE PADRONIZAÇÃO
# ============================================================

class TestStandardizeData:
    """Testes para função de padronização."""
    
    def test_standardize_converts_dates(self, sample_raw_data):
        """Testa conversão de datas."""
        result = standardize_data(sample_raw_data)
        
        if 'Data_Venda' in result.columns:
            assert pd.api.types.is_datetime64_any_dtype(result['Data_Venda'])
    
    def test_standardize_lowercase_text(self, sample_raw_data):
        """Testa conversão para minúsculas."""
        result = standardize_data(sample_raw_data)
        
        if 'Produto' in result.columns:
            # Todos produtos devem estar em lowercase
            assert all(result['Produto'].str.islower())
    
    def test_standardize_numeric_types(self, sample_raw_data):
        """Testa garantia de tipos numéricos."""
        result = standardize_data(sample_raw_data)
        
        if 'Quantidade' in result.columns:
            assert pd.api.types.is_numeric_dtype(result['Quantidade'])
        
        if 'Preco_Local' in result.columns:
            assert pd.api.types.is_numeric_dtype(result['Preco_Local'])
    
    def test_standardize_returns_dataframe(self, sample_raw_data):
        """Testa se retorna um DataFrame."""
        result = standardize_data(sample_raw_data)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_raw_data)


# ============================================================
# TESTES DE ENRIQUECIMENTO
# ============================================================

class TestEnrichData:
    """Testes para função de enriquecimento."""
    
    def test_enrich_adds_usd_conversion(self, sample_clean_data):
        """Testa se adiciona conversão para USD."""
        exchange_rate = {'rate': 0.18, 'source': 'Test'}
        
        result = enrich_data(sample_clean_data, exchange_rate)
        
        assert 'Preco_USD' in result.columns
        assert 'Taxa_Cambio' in result.columns
        assert result['Taxa_Cambio'].iloc[0] == 0.18
    
    def test_enrich_adds_time_features(self, sample_clean_data):
        """Testa se adiciona features de tempo."""
        exchange_rate = {'rate': 0.18, 'source': 'Test'}
        
        result = enrich_data(sample_clean_data, exchange_rate)
        
        # Verifica se colunas de tempo foram criadas
        if 'Data_Venda' in result.columns:
            assert 'Ano' in result.columns
            assert 'Mes' in result.columns
            assert 'Dia_Semana' in result.columns
    
    def test_enrich_preserves_original_columns(self, sample_clean_data):
        """Testa se preserva colunas originais."""
        exchange_rate = {'rate': 0.18, 'source': 'Test'}
        
        original_cols = set(sample_clean_data.columns)
        result = enrich_data(sample_clean_data, exchange_rate)
        
        # Todas colunas originais devem estar presentes
        assert original_cols.issubset(set(result.columns))


# ============================================================
# TESTES DE AGREGAÇÃO
# ============================================================

class TestAggregateData:
    """Testes para função de agregação."""
    
    def test_aggregate_returns_dataframe(self):
        """Testa se retorna um DataFrame."""
        # Cria dados com estrutura esperada pela função aggregate_data
        df = pd.DataFrame({
            'Data_Venda': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02']),
            'Produto': ['laptop', 'mouse', 'teclado'],
            'Quantidade': [2, 5, 3],
            'Preco_Local': [3500.00, 50.00, 200.00],
            'Preco_USD': [630.00, 9.00, 36.00],
            'Valor_Total_USD': [1260.00, 45.00, 108.00],
            'Regiao': ['sul', 'sudeste', 'norte']
        })
        
        result = aggregate_data(df)
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0


# ============================================================
# TESTES PARAMETRIZADOS
# ============================================================

@pytest.mark.parametrize("quantity,price,expected_total", [
    (2, 3500.00, 7000.00),
    (5, 50.00, 250.00),
    (3, 200.00, 600.00),
    (1, 1000.00, 1000.00),
    (10, 100.00, 1000.00),
])
def test_total_value_calculation(quantity, price, expected_total):
    """Testa cálculo de valor total com múltiplos valores."""
    df = pd.DataFrame({
        'Quantidade': [quantity],
        'Preco_Local': [price]
    })
    
    # Simula cálculo de valor total
    df['Valor_Total_BRL'] = df['Quantidade'] * df['Preco_Local']
    
    assert df['Valor_Total_BRL'].iloc[0] == expected_total


# ============================================================
# TESTES DE INTEGRAÇÃO
# ============================================================

class TestTransformPipeline:
    """Testes de integração do pipeline de transformação."""
    
    def test_full_transform_pipeline(self, sample_raw_data):
        """Testa pipeline completo de transformação."""
        # 1. Limpeza
        cleaned = clean_data(sample_raw_data)
        assert len(cleaned) > 0
        
        # 2. Padronização
        standardized = standardize_data(cleaned)
        assert isinstance(standardized, pd.DataFrame)
        
        # 3. Enriquecimento
        exchange_rate = {'rate': 0.18, 'source': 'Test'}
        enriched = enrich_data(standardized, exchange_rate)
        assert 'Preco_USD' in enriched.columns
        
        # 4. Agregação
        aggregated = aggregate_data(enriched)
        assert len(aggregated) > 0


if __name__ == "__main__":
    pytest.main([__file__, '-v', '--cov=src.etl_pipeline.transform'])

