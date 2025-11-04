"""
Testes básicos para o módulo de extração
"""

import pytest
from etl_scripts.extract import (
    validate_crypto_price,
    validate_exchange_rate,
    validate_api_response
)


class TestValidation:
    """Testes para funções de validação."""
    
    def test_validate_crypto_price_valid(self):
        """Testa validação de preço válido de Bitcoin."""
        assert validate_crypto_price(50000.0, 'BTC') == True
        assert validate_crypto_price(100000.0, 'BTC') == True
    
    def test_validate_crypto_price_invalid(self):
        """Testa validação de preço inválido de Bitcoin."""
        assert validate_crypto_price(5000.0, 'BTC') == False  # Muito baixo
        assert validate_crypto_price(600000.0, 'BTC') == False  # Muito alto
    
    def test_validate_exchange_rate_valid(self):
        """Testa validação de taxa de câmbio válida."""
        assert validate_exchange_rate(0.18, 'BRL', 'USD') == True
        assert validate_exchange_rate(0.20, 'BRL', 'USD') == True
    
    def test_validate_exchange_rate_invalid(self):
        """Testa validação de taxa de câmbio inválida."""
        assert validate_exchange_rate(0.05, 'BRL', 'USD') == False  # Muito baixa
        assert validate_exchange_rate(0.60, 'BRL', 'USD') == False  # Muito alta
    
    def test_validate_api_response_valid(self):
        """Testa validação de resposta de API válida."""
        data = {'rate': 0.18, 'date': '2024-01-01'}
        assert validate_api_response(data, ['rate', 'date'], 'TestAPI') == True
    
    def test_validate_api_response_invalid(self):
        """Testa validação de resposta de API inválida."""
        data = {'rate': 0.18}  # Faltando 'date'
        assert validate_api_response(data, ['rate', 'date'], 'TestAPI') == False


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
