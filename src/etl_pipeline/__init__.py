"""
ETL Pipeline Package
Pacote principal para o pipeline ETL de vendas multimoeda.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from .extract.extract import extract_all_sources
from .transform.transform import transform_data
from .load.load import load_all_data

__all__ = [
    'extract_all_sources',
    'transform_data',
    'load_all_data',
]
