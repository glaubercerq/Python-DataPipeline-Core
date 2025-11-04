"""
Módulo de Validação de Dados com Great Expectations
Garante qualidade e integridade dos dados em todas as fases do pipeline.

Validações implementadas:
- Schema: tipos de dados corretos
- Completude: campos obrigatórios não-nulos
- Consistência: valores dentro de ranges esperados
- Unicidade: chaves primárias sem duplicatas
"""

import pandas as pd
from typing import Dict, List, Tuple
from loguru import logger


class DataValidator:
    """Classe para validação de qualidade de dados."""
    
    def __init__(self):
        """Inicializa o validador."""
        self.validation_results = []
    
    def validate_schema(self, df: pd.DataFrame, expected_columns: List[str]) -> Tuple[bool, str]:
        """
        Valida se o DataFrame possui as colunas esperadas.
        
        Args:
            df: DataFrame a validar
            expected_columns: Lista de colunas esperadas
        
        Returns:
            Tupla (sucesso, mensagem)
        """
        missing_cols = set(expected_columns) - set(df.columns)
        extra_cols = set(df.columns) - set(expected_columns)
        
        if missing_cols:
            msg = f"Colunas faltando: {missing_cols}"
            logger.warning(f"⚠️ Validação Schema: {msg}")
            self.validation_results.append({"check": "schema", "status": "failed", "message": msg})
            return False, msg
        
        if extra_cols:
            msg = f"Colunas extras (não esperadas): {extra_cols}"
            logger.info(f"ℹ️ Validação Schema: {msg}")
        
        logger.success("✅ Validação Schema: Todas as colunas esperadas presentes")
        self.validation_results.append({"check": "schema", "status": "passed", "message": "OK"})
        return True, "OK"
    
    def validate_not_null(self, df: pd.DataFrame, columns: List[str]) -> Tuple[bool, str]:
        """
        Valida que colunas específicas não contenham valores nulos.
        
        Args:
            df: DataFrame a validar
            columns: Lista de colunas que não devem ser nulas
        
        Returns:
            Tupla (sucesso, mensagem)
        """
        null_counts = {}
        for col in columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count > 0:
                    null_counts[col] = null_count
        
        if null_counts:
            msg = f"Valores nulos encontrados: {null_counts}"
            logger.warning(f"⚠️ Validação Nulls: {msg}")
            self.validation_results.append({"check": "not_null", "status": "failed", "message": msg})
            return False, msg
        
        logger.success(f"✅ Validação Nulls: Colunas {columns} sem valores nulos")
        self.validation_results.append({"check": "not_null", "status": "passed", "message": "OK"})
        return True, "OK"
    
    def validate_data_types(self, df: pd.DataFrame, type_map: Dict[str, str]) -> Tuple[bool, str]:
        """
        Valida tipos de dados das colunas.
        
        Args:
            df: DataFrame a validar
            type_map: Dicionário {coluna: tipo_esperado}
                     Tipos: 'numeric', 'string', 'datetime', 'boolean'
        
        Returns:
            Tupla (sucesso, mensagem)
        """
        type_errors = []
        
        for col, expected_type in type_map.items():
            if col not in df.columns:
                continue
            
            if expected_type == 'numeric':
                if not pd.api.types.is_numeric_dtype(df[col]):
                    type_errors.append(f"{col} não é numérico")
            
            elif expected_type == 'string':
                if not pd.api.types.is_string_dtype(df[col]) and df[col].dtype != 'object':
                    type_errors.append(f"{col} não é string")
            
            elif expected_type == 'datetime':
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    type_errors.append(f"{col} não é datetime")
            
            elif expected_type == 'boolean':
                if not pd.api.types.is_bool_dtype(df[col]):
                    type_errors.append(f"{col} não é boolean")
        
        if type_errors:
            msg = f"Erros de tipo: {type_errors}"
            logger.warning(f"⚠️ Validação Tipos: {msg}")
            self.validation_results.append({"check": "data_types", "status": "failed", "message": msg})
            return False, msg
        
        logger.success("✅ Validação Tipos: Todos os tipos corretos")
        self.validation_results.append({"check": "data_types", "status": "passed", "message": "OK"})
        return True, "OK"
    
    def validate_value_range(self, df: pd.DataFrame, column: str, min_val=None, max_val=None) -> Tuple[bool, str]:
        """
        Valida se valores estão dentro de um range esperado.
        
        Args:
            df: DataFrame a validar
            column: Nome da coluna
            min_val: Valor mínimo aceitável
            max_val: Valor máximo aceitável
        
        Returns:
            Tupla (sucesso, mensagem)
        """
        if column not in df.columns:
            msg = f"Coluna {column} não encontrada"
            logger.warning(f"⚠️ Validação Range: {msg}")
            return False, msg
        
        out_of_range = []
        
        if min_val is not None:
            below_min = (df[column] < min_val).sum()
            if below_min > 0:
                out_of_range.append(f"{below_min} valores < {min_val}")
        
        if max_val is not None:
            above_max = (df[column] > max_val).sum()
            if above_max > 0:
                out_of_range.append(f"{above_max} valores > {max_val}")
        
        if out_of_range:
            msg = f"{column}: {', '.join(out_of_range)}"
            logger.warning(f"⚠️ Validação Range: {msg}")
            self.validation_results.append({"check": f"range_{column}", "status": "failed", "message": msg})
            return False, msg
        
        logger.success(f"✅ Validação Range: {column} dentro do esperado")
        self.validation_results.append({"check": f"range_{column}", "status": "passed", "message": "OK"})
        return True, "OK"
    
    def validate_no_duplicates(self, df: pd.DataFrame, columns: List[str] = None) -> Tuple[bool, str]:
        """
        Valida que não há duplicatas.
        
        Args:
            df: DataFrame a validar
            columns: Colunas para verificar duplicatas (None = todas)
        
        Returns:
            Tupla (sucesso, mensagem)
        """
        if columns:
            duplicates = df.duplicated(subset=columns).sum()
            subset_msg = f" nas colunas {columns}"
        else:
            duplicates = df.duplicated().sum()
            subset_msg = ""
        
        if duplicates > 0:
            msg = f"{duplicates} linhas duplicadas{subset_msg}"
            logger.warning(f"⚠️ Validação Duplicatas: {msg}")
            self.validation_results.append({"check": "duplicates", "status": "failed", "message": msg})
            return False, msg
        
        logger.success(f"✅ Validação Duplicatas: Nenhuma duplicata encontrada{subset_msg}")
        self.validation_results.append({"check": "duplicates", "status": "passed", "message": "OK"})
        return True, "OK"
    
    def get_summary(self) -> Dict:
        """
        Retorna resumo das validações executadas.
        
        Returns:
            Dicionário com estatísticas das validações
        """
        total = len(self.validation_results)
        passed = sum(1 for r in self.validation_results if r["status"] == "passed")
        failed = total - passed
        
        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "success_rate": f"{(passed/total*100):.1f}%" if total > 0 else "0%",
            "results": self.validation_results
        }
    
    def reset(self):
        """Limpa resultados de validações anteriores."""
        self.validation_results = []


# ============================================================
# VALIDAÇÕES PRÉ-DEFINIDAS PARA O PIPELINE
# ============================================================

def validate_sales_data(df: pd.DataFrame) -> Dict:
    """
    Valida dados de vendas extraídos.
    
    Args:
        df: DataFrame de vendas
    
    Returns:
        Dicionário com resultados das validações
    """
    logger.info("🔍 Iniciando validação de dados de vendas...")
    validator = DataValidator()
    
    # 1. Validar schema
    expected_cols = ['Data_Venda', 'Produto', 'Categoria', 'Quantidade', 
                     'Preco_Local', 'Regiao', 'Vendedor']
    validator.validate_schema(df, expected_cols)
    
    # 2. Validar campos não-nulos críticos
    required_cols = ['Data_Venda', 'Produto', 'Preco_Local']
    validator.validate_not_null(df, required_cols)
    
    # 3. Validar tipos de dados
    type_map = {
        'Quantidade': 'numeric',
        'Preco_Local': 'numeric',
        'Produto': 'string',
        'Categoria': 'string',
        'Regiao': 'string'
    }
    validator.validate_data_types(df, type_map)
    
    # 4. Validar ranges de valores
    if 'Quantidade' in df.columns:
        validator.validate_value_range(df, 'Quantidade', min_val=1, max_val=10000)
    
    if 'Preco_Local' in df.columns:
        validator.validate_value_range(df, 'Preco_Local', min_val=0.01, max_val=1000000)
    
    # 5. Verificar duplicatas
    validator.validate_no_duplicates(df)
    
    summary = validator.get_summary()
    logger.info(f"📋 Validação concluída: {summary['passed']}/{summary['total_checks']} checks passaram")
    
    return summary


def validate_transformed_data(df: pd.DataFrame) -> Dict:
    """
    Valida dados após transformação.
    
    Args:
        df: DataFrame transformado
    
    Returns:
        Dicionário com resultados das validações
    """
    logger.info("🔍 Validando dados transformados...")
    validator = DataValidator()
    
    # Validações específicas pós-transformação
    if 'Valor_Total_BRL' in df.columns:
        validator.validate_not_null(df, ['Valor_Total_BRL'])
        validator.validate_value_range(df, 'Valor_Total_BRL', min_val=0)
    
    if 'Valor_USD' in df.columns:
        validator.validate_not_null(df, ['Valor_USD'])
        validator.validate_value_range(df, 'Valor_USD', min_val=0)
    
    if 'Valor_BTC' in df.columns:
        validator.validate_value_range(df, 'Valor_BTC', min_val=0, max_val=100)
    
    summary = validator.get_summary()
    logger.info(f"📋 Validação concluída: {summary['passed']}/{summary['total_checks']} checks passaram")
    
    return summary


__all__ = [
    'DataValidator',
    'validate_sales_data',
    'validate_transformed_data'
]
