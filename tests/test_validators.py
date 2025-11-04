"""
Testes para o módulo de validação de dados
Testa DataValidator e validações pré-definidas
"""

import pytest
import pandas as pd
from src.etl_pipeline.utils.validators import (
    DataValidator,
    validate_sales_data,
    validate_transformed_data
)


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def valid_sales_dataframe():
    """DataFrame de vendas válido."""
    return pd.DataFrame({
        'Data_Venda': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'Produto': ['Laptop', 'Mouse', 'Teclado'],
        'Categoria': ['Eletrônicos', 'Periféricos', 'Periféricos'],
        'Quantidade': [2, 5, 3],
        'Preco_Local': [3500.00, 50.00, 200.00],
        'Regiao': ['Sul', 'Sudeste', 'Norte'],
        'Vendedor': ['João', 'Maria', 'Pedro']
    })


@pytest.fixture
def invalid_sales_dataframe_missing_cols():
    """DataFrame com colunas faltando."""
    return pd.DataFrame({
        'Produto': ['Laptop', 'Mouse'],
        'Preco_Local': [3500.00, 50.00]
        # Faltando: Data_Venda, Categoria, Quantidade, etc.
    })


@pytest.fixture
def invalid_sales_dataframe_nulls():
    """DataFrame com valores nulos em campos críticos."""
    return pd.DataFrame({
        'Data_Venda': pd.to_datetime(['2024-01-01', None, '2024-01-03']),
        'Produto': ['Laptop', 'Mouse', None],
        'Categoria': ['Eletrônicos', 'Periféricos', 'Periféricos'],
        'Quantidade': [2, 5, 3],
        'Preco_Local': [3500.00, None, 200.00],  # Preço nulo
        'Regiao': ['Sul', 'Sudeste', 'Norte'],
        'Vendedor': ['João', 'Maria', 'Pedro']
    })


@pytest.fixture
def invalid_sales_dataframe_wrong_types():
    """DataFrame com tipos de dados incorretos."""
    return pd.DataFrame({
        'Data_Venda': pd.to_datetime(['2024-01-01', '2024-01-02', '2024-01-03']),
        'Produto': ['Laptop', 'Mouse', 'Teclado'],
        'Categoria': ['Eletrônicos', 'Periféricos', 'Periféricos'],
        'Quantidade': ['dois', 'cinco', 'três'],  # String em vez de número
        'Preco_Local': [3500.00, 50.00, 200.00],
        'Regiao': ['Sul', 'Sudeste', 'Norte'],
        'Vendedor': ['João', 'Maria', 'Pedro']
    })


@pytest.fixture
def dataframe_with_duplicates():
    """DataFrame com linhas duplicadas."""
    return pd.DataFrame({
        'Data_Venda': pd.to_datetime(['2024-01-01', '2024-01-01', '2024-01-02']),
        'Produto': ['Laptop', 'Laptop', 'Mouse'],
        'Categoria': ['Eletrônicos', 'Eletrônicos', 'Periféricos'],
        'Quantidade': [2, 2, 5],
        'Preco_Local': [3500.00, 3500.00, 50.00],
        'Regiao': ['Sul', 'Sul', 'Sudeste'],
        'Vendedor': ['João', 'João', 'Maria']
    })


# ============================================================
# TESTES DO DATA VALIDATOR
# ============================================================

class TestDataValidator:
    """Testes para a classe DataValidator."""
    
    def test_validator_initialization(self):
        """Testa inicialização do validador."""
        validator = DataValidator()
        assert validator.validation_results == []
    
    def test_validate_schema_success(self, valid_sales_dataframe):
        """Testa validação de schema bem-sucedida."""
        validator = DataValidator()
        expected_cols = ['Data_Venda', 'Produto', 'Preco_Local']
        
        success, msg = validator.validate_schema(valid_sales_dataframe, expected_cols)
        
        assert success == True
        assert msg == "OK"
    
    def test_validate_schema_missing_columns(self, invalid_sales_dataframe_missing_cols):
        """Testa validação de schema com colunas faltando."""
        validator = DataValidator()
        expected_cols = ['Data_Venda', 'Produto', 'Preco_Local', 'Quantidade']
        
        success, msg = validator.validate_schema(invalid_sales_dataframe_missing_cols, expected_cols)
        
        assert success == False
        assert "faltando" in msg.lower()
    
    def test_validate_not_null_success(self, valid_sales_dataframe):
        """Testa validação de não-nulos bem-sucedida."""
        validator = DataValidator()
        required_cols = ['Produto', 'Preco_Local']
        
        success, msg = validator.validate_not_null(valid_sales_dataframe, required_cols)
        
        assert success == True
    
    def test_validate_not_null_failure(self, invalid_sales_dataframe_nulls):
        """Testa validação de não-nulos com falha."""
        validator = DataValidator()
        required_cols = ['Produto', 'Preco_Local']
        
        success, msg = validator.validate_not_null(invalid_sales_dataframe_nulls, required_cols)
        
        assert success == False
        assert "nulos" in msg.lower()
    
    def test_validate_data_types_success(self, valid_sales_dataframe):
        """Testa validação de tipos bem-sucedida."""
        validator = DataValidator()
        type_map = {
            'Quantidade': 'numeric',
            'Preco_Local': 'numeric',
            'Produto': 'string'
        }
        
        success, msg = validator.validate_data_types(valid_sales_dataframe, type_map)
        
        assert success == True
    
    def test_validate_value_range_success(self, valid_sales_dataframe):
        """Testa validação de range bem-sucedida."""
        validator = DataValidator()
        
        success, msg = validator.validate_value_range(
            valid_sales_dataframe, 
            'Quantidade', 
            min_val=1, 
            max_val=10000
        )
        
        assert success == True
    
    def test_validate_value_range_failure(self, valid_sales_dataframe):
        """Testa validação de range com falha."""
        validator = DataValidator()
        
        # Range muito restrito (vai falhar)
        success, msg = validator.validate_value_range(
            valid_sales_dataframe, 
            'Quantidade', 
            min_val=10,  # Valor mínimo muito alto
            max_val=20
        )
        
        assert success == False
    
    def test_validate_no_duplicates_success(self, valid_sales_dataframe):
        """Testa validação de duplicatas bem-sucedida."""
        validator = DataValidator()
        
        success, msg = validator.validate_no_duplicates(valid_sales_dataframe)
        
        assert success == True
    
    def test_validate_no_duplicates_failure(self, dataframe_with_duplicates):
        """Testa validação de duplicatas com falha."""
        validator = DataValidator()
        
        success, msg = validator.validate_no_duplicates(dataframe_with_duplicates)
        
        assert success == False
        assert "duplicadas" in msg.lower()
    
    def test_get_summary(self, valid_sales_dataframe):
        """Testa geração de resumo."""
        validator = DataValidator()
        
        validator.validate_schema(valid_sales_dataframe, ['Produto'])
        validator.validate_not_null(valid_sales_dataframe, ['Produto'])
        
        summary = validator.get_summary()
        
        assert summary['total_checks'] == 2
        assert summary['passed'] == 2
        assert summary['failed'] == 0
        assert summary['success_rate'] == "100.0%"
    
    def test_reset(self, valid_sales_dataframe):
        """Testa reset do validador."""
        validator = DataValidator()
        
        validator.validate_schema(valid_sales_dataframe, ['Produto'])
        assert len(validator.validation_results) > 0
        
        validator.reset()
        assert len(validator.validation_results) == 0


# ============================================================
# TESTES DE VALIDAÇÕES PRÉ-DEFINIDAS
# ============================================================

class TestPredefinedValidations:
    """Testes para funções de validação pré-definidas."""
    
    def test_validate_sales_data_success(self, valid_sales_dataframe):
        """Testa validação completa de dados de vendas válidos."""
        summary = validate_sales_data(valid_sales_dataframe)
        
        assert summary['total_checks'] > 0
        assert summary['passed'] > 0
        assert summary['success_rate'] is not None
    
    def test_validate_sales_data_with_issues(self, invalid_sales_dataframe_nulls):
        """Testa validação de dados de vendas com problemas."""
        summary = validate_sales_data(invalid_sales_dataframe_nulls)
        
        assert summary['total_checks'] > 0
        assert summary['failed'] > 0  # Deve ter falhas
    
    def test_validate_transformed_data(self):
        """Testa validação de dados transformados."""
        # Cria DataFrame transformado simulado
        df_transformed = pd.DataFrame({
            'Produto': ['Laptop', 'Mouse'],
            'Valor_Total_BRL': [7000.00, 250.00],
            'Valor_USD': [1260.00, 45.00],
            'Valor_BTC': [0.0252, 0.0009]
        })
        
        summary = validate_transformed_data(df_transformed)
        
        assert summary['total_checks'] > 0
        assert 'passed' in summary
        assert 'failed' in summary


# ============================================================
# TESTES PARAMETRIZADOS
# ============================================================

@pytest.mark.parametrize("column,min_val,max_val,should_pass", [
    ('Quantidade', 1, 10, True),
    ('Quantidade', 10, 20, False),  # Range muito restrito
    ('Preco_Local', 0, 10000, True),
    ('Preco_Local', 1000, 2000, False),  # Range muito restrito
])
def test_value_range_parametrized(valid_sales_dataframe, column, min_val, max_val, should_pass):
    """Testa validação de range com múltiplos parâmetros."""
    validator = DataValidator()
    
    success, _ = validator.validate_value_range(
        valid_sales_dataframe, 
        column, 
        min_val=min_val, 
        max_val=max_val
    )
    
    assert success == should_pass


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
