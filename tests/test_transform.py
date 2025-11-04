"""
Testes básicos para o módulo de transformação
"""

import pytest
import pandas as pd
from etl_scripts.transform import clean_data, standardize_data


class TestTransform:
    """Testes para funções de transformação."""
    
    def test_clean_data_removes_duplicates(self):
        """Testa remoção de duplicatas."""
        df = pd.DataFrame({
            'Produto': ['Mouse', 'Mouse', 'Teclado'],
            'Preco_Local': [50.0, 50.0, 100.0]
        })
        
        df_clean = clean_data(df)
        assert len(df_clean) == 2  # Uma duplicata removida
    
    def test_standardize_data_converts_dates(self):
        """Testa conversão de datas."""
        df = pd.DataFrame({
            'Data_Venda': ['2024-01-01', '2024-01-02'],
            'Produto': ['Mouse', 'Teclado']
        })
        
        df_std = standardize_data(df)
        assert pd.api.types.is_datetime64_any_dtype(df_std['Data_Venda'])
    
    def test_standardize_data_lowercase_text(self):
        """Testa conversão para lowercase."""
        df = pd.DataFrame({
            'Produto': ['MOUSE', 'TECLADO'],
            'Categoria': ['ELETRONICOS', 'ACESSORIOS']
        })
        
        df_std = standardize_data(df)
        assert all(df_std['Produto'].str.islower())
        assert all(df_std['Categoria'].str.islower())


if __name__ == "__main__":
    pytest.main([__file__, '-v'])
