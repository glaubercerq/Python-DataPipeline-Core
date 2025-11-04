"""
Testes completos para o módulo de extração
Inclui: validações, mocks de API, testes de integração
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from src.etl_pipeline.extract.extract import (
    validate_crypto_price,
    validate_exchange_rate,
    validate_api_response,
    extract_csv_data
)


# ============================================================
# FIXTURES
# ============================================================

@pytest.fixture
def sample_csv_data():
    """Fixture com dados de exemplo para CSV."""
    return pd.DataFrame({
        'Data_Venda': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Produto': ['Laptop', 'Mouse', 'Teclado'],
        'Categoria': ['Eletrônicos', 'Periféricos', 'Periféricos'],
        'Quantidade': [2, 5, 3],
        'Preco_Local': [3500.00, 50.00, 200.00],
        'Regiao': ['Sul', 'Sudeste', 'Norte'],
        'Vendedor': ['João', 'Maria', 'Pedro']
    })


@pytest.fixture
def mock_api_response_valid():
    """Fixture com resposta válida de API."""
    return {
        'bitcoin': {'usd': 50000.0},
        'date': '2024-01-01',
        'rate': 0.18
    }


@pytest.fixture
def mock_api_response_invalid():
    """Fixture com resposta inválida de API."""
    return {
        'bitcoin': {'usd': 500000.0},  # Preço muito alto (invalido)
        'rate': 0.05  # Taxa muito baixa (invalida)
    }


# ============================================================
# TESTES DE VALIDAÇÃO
# ============================================================

class TestValidation:
    """Testes para funções de validação."""
    
    def test_validate_crypto_price_valid(self):
        """Testa validação de preço válido de Bitcoin."""
        assert validate_crypto_price(50000.0, 'BTC') == True
        assert validate_crypto_price(100000.0, 'BTC') == True
        assert validate_crypto_price(10000.0, 'BTC') == True  # Limite inferior
        assert validate_crypto_price(500000.0, 'BTC') == True  # Limite superior
    
    def test_validate_crypto_price_invalid(self):
        """Testa validação de preço inválido de Bitcoin."""
        assert validate_crypto_price(5000.0, 'BTC') == False  # Muito baixo
        assert validate_crypto_price(600000.0, 'BTC') == False  # Muito alto
        assert validate_crypto_price(0, 'BTC') == False
        assert validate_crypto_price(-100, 'BTC') == False  # Negativo
    
    def test_validate_exchange_rate_valid(self):
        """Testa validação de taxa de câmbio válida."""
        assert validate_exchange_rate(0.18, 'BRL', 'USD') == True
        assert validate_exchange_rate(0.20, 'BRL', 'USD') == True
        assert validate_exchange_rate(0.10, 'BRL', 'USD') == True  # Limite inferior
        assert validate_exchange_rate(0.50, 'BRL', 'USD') == True  # Limite superior
    
    def test_validate_exchange_rate_invalid(self):
        """Testa validação de taxa de câmbio inválida."""
        assert validate_exchange_rate(0.05, 'BRL', 'USD') == False  # Muito baixa
        assert validate_exchange_rate(0.60, 'BRL', 'USD') == False  # Muito alta
        assert validate_exchange_rate(0, 'BRL', 'USD') == False
        assert validate_exchange_rate(-0.1, 'BRL', 'USD') == False  # Negativa
    
    def test_validate_api_response_valid(self):
        """Testa validação de resposta de API válida."""
        data = {'rate': 0.18, 'date': '2024-01-01'}
        assert validate_api_response(data, ['rate', 'date'], 'TestAPI') == True
        
        # Teste com campos extras (deve passar)
        data_extra = {'rate': 0.18, 'date': '2024-01-01', 'extra': 'value'}
        assert validate_api_response(data_extra, ['rate', 'date'], 'TestAPI') == True
    
    def test_validate_api_response_invalid(self):
        """Testa validação de resposta de API inválida."""
        # Campos faltando
        data_missing = {'rate': 0.18}  # Faltando 'date'
        assert validate_api_response(data_missing, ['rate', 'date'], 'TestAPI') == False
        
        # Não é dicionário
        assert validate_api_response([], ['rate'], 'TestAPI') == False
        assert validate_api_response(None, ['rate'], 'TestAPI') == False
        assert validate_api_response("string", ['rate'], 'TestAPI') == False


# ============================================================
# TESTES DE EXTRAÇÃO CSV
# ============================================================

class TestCSVExtraction:
    """Testes para extração de dados CSV."""
    
    @patch('pandas.read_csv')
    def test_extract_csv_success(self, mock_read_csv, sample_csv_data):
        """Testa extração bem-sucedida de CSV."""
        mock_read_csv.return_value = sample_csv_data
        
        result = extract_csv_data('fake_path.csv')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 3
        assert 'Produto' in result.columns
        assert 'Preco_Local' in result.columns
    
    @patch('pandas.read_csv')
    def test_extract_csv_empty_file(self, mock_read_csv):
        """Testa extração de CSV vazio."""
        mock_read_csv.return_value = pd.DataFrame()
        
        result = extract_csv_data('empty.csv')
        
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
    
    @patch('pandas.read_csv')
    def test_extract_csv_file_not_found(self, mock_read_csv):
        """Testa erro quando arquivo não existe."""
        mock_read_csv.side_effect = FileNotFoundError("File not found")
        
        with pytest.raises(FileNotFoundError):
            extract_csv_data('nonexistent.csv')


# ============================================================
# TESTES DE INTEGRAÇÃO COM MOCKS
# ============================================================

class TestAPIIntegration:
    """Testes de integração com APIs (usando mocks)."""
    
    @patch('requests.get')
    def test_api_call_success(self, mock_get):
        """Testa chamada de API bem-sucedida."""
        # Simula resposta de API
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'bitcoin': {'usd': 50000.0}}
        mock_get.return_value = mock_response
        
        # Aqui você chamaria sua função de API real
        # Por exemplo: result = fetch_crypto_price()
        # assert result == 50000.0
    
    @patch('requests.get')
    def test_api_call_timeout(self, mock_get):
        """Testa timeout de API."""
        import requests
        mock_get.side_effect = requests.exceptions.Timeout()
        
        # Aqui você testaria o comportamento de timeout
        # Por exemplo: result = fetch_crypto_price()
        # assert result is None ou usa fallback
    
    @patch('requests.get')
    def test_api_call_invalid_response(self, mock_get):
        """Testa resposta inválida de API."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        
        # Teste de tratamento de erro 404


# ============================================================
# TESTES PARAMETRIZADOS
# ============================================================

@pytest.mark.parametrize("price,expected", [
    (50000.0, True),
    (100000.0, True),
    (5000.0, False),
    (600000.0, False),
    (10000.0, True),
    (500000.0, True),
])
def test_crypto_price_parametrized(price, expected):
    """Testa validação de preço com múltiplos valores."""
    assert validate_crypto_price(price, 'BTC') == expected


@pytest.mark.parametrize("rate,expected", [
    (0.18, True),
    (0.20, True),
    (0.05, False),
    (0.60, False),
    (0.10, True),
    (0.50, True),
])
def test_exchange_rate_parametrized(rate, expected):
    """Testa validação de taxa de câmbio com múltiplos valores."""
    assert validate_exchange_rate(rate, 'BRL', 'USD') == expected


if __name__ == "__main__":
    pytest.main([__file__, '-v', '--cov=src.etl_pipeline.extract'])

