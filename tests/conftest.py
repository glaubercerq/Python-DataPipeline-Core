"""
Configurações compartilhadas para testes pytest
Define fixtures globais e configurações de teste
"""

import pytest
import pandas as pd
from pathlib import Path
import sys

# Adiciona o diretório raiz ao PYTHONPATH para imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# ============================================================
# FIXTURES GLOBAIS
# ============================================================

@pytest.fixture(scope="session")
def project_root_path():
    """Retorna o caminho raiz do projeto."""
    return Path(__file__).parent.parent


@pytest.fixture(scope="session")
def test_data_dir(project_root_path):
    """Retorna o diretório de dados de teste."""
    test_dir = project_root_path / "tests" / "test_data"
    test_dir.mkdir(exist_ok=True)
    return test_dir


@pytest.fixture
def sample_exchange_rate():
    """Taxa de câmbio padrão para testes."""
    return 0.18  # 1 BRL = 0.18 USD


@pytest.fixture
def sample_btc_price():
    """Preço do Bitcoin padrão para testes."""
    return 50000.0  # USD


@pytest.fixture
def sample_api_timeout():
    """Timeout padrão para chamadas de API em testes."""
    return 5  # segundos


# ============================================================
# CONFIGURAÇÕES DE TESTE
# ============================================================

# Removido reset_pandas_options pois causa erro com matplotlib

@pytest.fixture(autouse=True)
def suppress_loguru_in_tests(monkeypatch):
    """Suprime logs do Loguru durante testes para output limpo."""
    # Você pode descomentar isso se quiser silenciar completamente os logs nos testes
    # from loguru import logger
    # logger.remove()
    pass


# ============================================================
# MARKERS CUSTOMIZADOS
# ============================================================

def pytest_configure(config):
    """Configura markers customizados."""
    config.addinivalue_line(
        "markers", "slow: marca testes lentos (integração, API real)"
    )
    config.addinivalue_line(
        "markers", "integration: marca testes de integração"
    )
    config.addinivalue_line(
        "markers", "unit: marca testes unitários"
    )
    config.addinivalue_line(
        "markers", "api: marca testes que usam APIs externas"
    )
