"""
ETL Scripts Package
Módulos de Extract, Transform e Load para pipeline de dados
"""

__version__ = '1.0.0'
__author__ = 'Data Engineering Team'

# Importações facilitadas
from .extract import extract_all_sources, extract_csv_data, extract_exchange_rate_api, extract_crypto_price_api
from .transform import transform_data, clean_data, standardize_data, enrich_data, aggregate_data
from .load import load_all_data, create_database_connection, load_to_database

__all__ = [
    'extract_all_sources',
    'extract_csv_data',
    'extract_exchange_rate_api',
    'extract_crypto_price_api',
    'transform_data',
    'clean_data',
    'standardize_data',
    'enrich_data',
    'aggregate_data',
    'load_all_data',
    'create_database_connection',
    'load_to_database'
]
