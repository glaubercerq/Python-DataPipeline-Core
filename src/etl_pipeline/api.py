"""
API REST para Pipeline ETL
Permite testar cada camada (Extract, Transform, Load) via endpoints HTTP.

Funcionalidades:
- Upload de arquivos (CSV, JSON, XML)
- Configuração dinâmica de APIs
- Execução por camadas ou pipeline completo
- Retorno de resultados em JSON
- Documentação Swagger automática
"""

from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from typing import List, Optional
import pandas as pd
import json
import io
import xml.etree.ElementTree as ET
from datetime import datetime
import numpy as np
import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
root_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(root_dir))

from src.etl_pipeline.extract.extract import (
    extract_csv_data,
    extract_exchange_rate_api,
    extract_crypto_price_api
)
from src.etl_pipeline.transform.transform import (
    clean_data,
    standardize_data,
    enrich_data,
    aggregate_data
)
from src.etl_pipeline.load.load import (
    create_database_connection,
    load_all_data,
    load_to_database
)
from src.etl_pipeline.utils.validators import validate_sales_data, validate_transformed_data
from sqlalchemy import text

# ============================================================
# INICIALIZAÇÃO DA API
# ============================================================

app = FastAPI(
    title="ETL Pipeline API",
    description="Execução e teste do Pipeline ETL de Vendas",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS para permitir requisições de qualquer origem
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def parse_file(file: UploadFile) -> pd.DataFrame:
    """
    Faz parse de arquivos CSV, JSON ou XML para DataFrame.
    
    Args:
        file: Arquivo enviado via upload
    
    Returns:
        DataFrame com os dados
    
    Raises:
        HTTPException: Se formato não suportado
    """
    content = file.file.read()
    filename = file.filename.lower()
    
    try:
        if filename.endswith('.csv'):
            return pd.read_csv(io.BytesIO(content))
        
        elif filename.endswith('.json'):
            data = json.loads(content.decode('utf-8'))
            if isinstance(data, list):
                return pd.DataFrame(data)
            else:
                return pd.DataFrame([data])
        
        elif filename.endswith('.xml'):
            tree = ET.parse(io.BytesIO(content))
            root = tree.getroot()
            data = []
            for child in root:
                row = {elem.tag: elem.text for elem in child}
                data.append(row)
            df = pd.DataFrame(data)
            
            # Converte tipos de dados corretamente (XML vem como string)
            if 'Quantidade' in df.columns:
                df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce')
            if 'Preco_Local' in df.columns:
                df['Preco_Local'] = pd.to_numeric(df['Preco_Local'], errors='coerce')
            
            return df
        
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Formato não suportado: {filename}. Use CSV, JSON ou XML."
            )
    
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Erro ao processar arquivo: {str(e)}"
        )


def dataframe_to_dict(df: pd.DataFrame, max_rows: int = 100) -> dict:
    """
    Converte DataFrame para dicionário JSON-serializável.
    Usa conversão completa para tipos Python nativos.
    
    Args:
        df: DataFrame a converter
        max_rows: Número máximo de linhas a retornar
    
    Returns:
        Dicionário com metadados e dados
    """
    # Cria cópia para não modificar o original
    df_subset = df.head(max_rows).copy()
    
    # Converte DataFrame para JSON usando pandas (que sabe lidar com Timestamp)
    # depois reconverte para dict Python
    json_str = df_subset.to_json(orient='records', date_format='iso')
    data_records = json.loads(json_str)
    
    return {
        "total_rows": int(len(df)),
        "total_columns": int(len(df.columns)),
        "columns": list(df.columns),
        "data": data_records,
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
    }


# ============================================================
# ENDPOINTS - HEALTH CHECK
# ============================================================

@app.get("/", tags=["Status"])
async def root():
    """Endpoint raiz - verifica se a API está funcionando."""
    return {
        "message": "ETL Pipeline API - Funcionando",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "endpoints": {
            "docs": "/docs",
            "health": "/health",
            "extract": "/extract",
            "transform": "/transform",
            "load": "/load",
            "pipeline": "/pipeline"
        }
    }


@app.get("/health", tags=["Status"])
async def health_check():
    """Verifica saúde da API e conexão com banco de dados."""
    try:
        # Testa conexão com banco
        engine = create_database_connection()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception as e:
        db_status = f"error: {str(e)}"
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "database": db_status
    }


# ============================================================
# ENDPOINTS - EXTRACT
# ============================================================

@app.post("/extract", tags=["Extract"])
async def extract_data(
    file: UploadFile = File(..., description="Arquivo CSV, JSON ou XML com dados de vendas"),
    validate: bool = Query(True, description="Validar dados após extração")
):
    """
    Extrai dados de um arquivo e opcionalmente valida.
    
    Suporta:
    - CSV (.csv)
    - JSON (.json)
    - XML (.xml)
    
    Retorna:
    - Dados extraídos
    - Estatísticas básicas
    - Validação (opcional)
    """
    try:
        # Parse do arquivo
        df = parse_file(file)
        
        # Validação
        validation_result = None
        if validate:
            validation_result = validate_sales_data(df)
        
        # Retorno
        return JSONResponse(content={
            "status": "success",
            "message": f"Dados extraídos de {file.filename}",
            "source_file": file.filename,
            "source_format": file.filename.split('.')[-1].upper(),
            "extracted": dataframe_to_dict(df),
            "validation": validation_result
        })
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na extração: {str(e)}")


@app.get("/extract/apis", tags=["Extract"])
async def extract_apis(
    exchange_apis: List[str] = Query(
        ["frankfurter"], 
        description="APIs de câmbio a usar"
    ),
    crypto_apis: List[str] = Query(
        ["coingecko"], 
        description="APIs de criptomoeda a usar"
    )
):
    """
    Extrai dados de APIs externas (taxa de câmbio e Bitcoin).
    
    APIs Disponíveis:
    - Câmbio: frankfurter, exchangerate, fixer
    - Crypto: coingecko, binance, coincap, coindesk
    
    Retorna:
    - Taxa de câmbio BRL→USD
    - Cotação Bitcoin em USD
    """
    try:
        # Extrai taxa de câmbio
        exchange_rate = extract_exchange_rate_api()
        
        # Extrai cotação Bitcoin
        btc_price = extract_crypto_price_api()
        
        return JSONResponse(content={
            "status": "success",
            "exchange_rate": {
                "base": "BRL",
                "target": "USD",
                "rate": exchange_rate.get('rate'),
                "source": exchange_rate.get('source')
            },
            "bitcoin": {
                "price_usd": btc_price.get('price'),
                "source": btc_price.get('source')
            },
            "timestamp": datetime.now().isoformat()
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao extrair APIs: {str(e)}")


# ============================================================
# ENDPOINTS - TRANSFORM
# ============================================================

@app.post("/transform", tags=["Transform"])
async def transform_data_endpoint(
    file: UploadFile = File(..., description="Arquivo com dados a transformar"),
    exchange_rate: Optional[float] = Form(None, description="Taxa de câmbio customizada"),
    skip_cleaning: bool = Form(False, description="Pular etapa de limpeza"),
    skip_standardization: bool = Form(False, description="Pular etapa de padronização"),
    skip_enrichment: bool = Form(False, description="Pular etapa de enriquecimento"),
    validate: bool = Form(True, description="Validar dados transformados")
):
    """
    Transforma dados com opções configuráveis.
    
    Etapas (todas opcionais):
    1. Limpeza (duplicatas, nulos, espaços)
    2. Padronização (tipos, formatos)
    3. Enriquecimento (conversões, features)
    
    Retorna:
    - Dados transformados
    - Estatísticas de transformação
    - Validação (opcional)
    """
    try:
        # Parse do arquivo
        df = parse_file(file)
        
        # Aplica transformações conforme configuração
        if not skip_cleaning:
            df = clean_data(df)
        
        if not skip_standardization:
            df = standardize_data(df)
        
        if not skip_enrichment:
            # Usa taxa de câmbio customizada ou extrai de API
            if exchange_rate is None:
                exchange_data = extract_exchange_rate_api()
            else:
                exchange_data = {'rate': exchange_rate, 'source': 'custom'}
            
            df = enrich_data(df, exchange_data)
        
        # Validação
        validation_result = None
        if validate:
            validation_result = validate_transformed_data(df)
        
        return JSONResponse(content={
            "status": "success",
            "message": "Dados transformados com sucesso",
            "transformations_applied": {
                "cleaning": not skip_cleaning,
                "standardization": not skip_standardization,
                "enrichment": not skip_enrichment
            },
            "transformed": dataframe_to_dict(df, max_rows=50),
            "validation": validation_result
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na transformação: {str(e)}")


@app.post("/transform/aggregate", tags=["Transform"])
async def aggregate_data_endpoint(
    file: UploadFile = File(..., description="Arquivo com dados de vendas"),
    exchange_rate: float = Form(1.0, description="Taxa de câmbio BRL->USD")
):
    """
    Processa e agrega dados de vendas por período e produto.
    
    Fluxo: Clean -> Standardize -> Enrich -> Aggregate
    
    Retorna:
    - Dados agregados
    - Totais por período
    """
    try:
        df = parse_file(file)
        
        # Valida dados de entrada
        validate_sales_data(df)
        
        # Processa os dados antes de agregar
        df_clean = clean_data(df)
        df_std = standardize_data(df_clean)
        df_enriched = enrich_data(df_std, exchange_rate={'rate': exchange_rate})
        
        # Agrega os dados enriquecidos
        df_aggregated = aggregate_data(df_enriched)
        
        return JSONResponse(content={
            "status": "success",
            "message": "Dados agregados com sucesso",
            "aggregated": dataframe_to_dict(df_aggregated),
            "summary": {
                "total_periods": len(df_aggregated),
                "original_records": len(df),
                "exchange_rate_used": exchange_rate
            }
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro na agregação: {str(e)}")


# ============================================================
# ENDPOINTS - LOAD
# ============================================================

@app.post("/load", tags=["Load"])
async def load_data_endpoint(
    file: UploadFile = File(..., description="Arquivo com dados a carregar"),
    table_name: str = Form("vendas_api", description="Nome da tabela"),
    mode: str = Form("replace", description="Modo de carga (replace, append)")
):
    """
    Carrega dados no banco de dados PostgreSQL.
    
    Modos:
    - replace: Substitui tabela existente
    - append: Adiciona à tabela existente
    
    Retorna:
    - Status da carga
    - Registros carregados
    """
    try:
        df = parse_file(file)
        
        # Cria conexão
        engine = create_database_connection()
        
        # Carrega no banco
        load_to_database(df, table_name, engine, load_mode=mode)
        
        return JSONResponse(content={
            "status": "success",
            "message": f"Dados carregados na tabela '{table_name}'",
            "records_loaded": len(df),
            "mode": mode,
            "timestamp": datetime.now().isoformat()
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no carregamento: {str(e)}")


# ============================================================
# ENDPOINTS - PIPELINE COMPLETO
# ============================================================

@app.post("/pipeline", tags=["Pipeline"])
async def run_full_pipeline(
    file: UploadFile = File(..., description="Arquivo de entrada"),
    detailed_table: str = Form("vendas_detalhadas_api", description="Tabela de dados detalhados"),
    aggregated_table: str = Form("vendas_agregadas_api", description="Tabela de dados agregados"),
    exchange_rate: Optional[float] = Form(None, description="Taxa de câmbio customizada"),
    load_to_db: bool = Form(True, description="Carregar no banco de dados")
):
    """
    Executa o pipeline ETL completo: Extract → Transform → Load.
    
    Fluxo:
    1. Extração do arquivo
    2. Limpeza de dados
    3. Padronização
    4. Enriquecimento (com APIs)
    5. Agregação
    6. Carregamento no banco (opcional)
    
    Retorna:
    - Resumo completo da execução
    - Dados transformados e agregados
    - Validações
    """
    try:
        start_time = datetime.now()
        
        # EXTRACT
        df_raw = parse_file(file)
        
        # Verifica se tem as colunas mínimas necessárias
        required_columns = ['Data_Venda', 'Produto', 'Preco_Local', 'Quantidade']
        missing_columns = [col for col in required_columns if col not in df_raw.columns]
        
        if missing_columns:
            return JSONResponse(
                status_code=400,
                content={
                    "status": "error",
                    "message": f"Arquivo inválido. Colunas obrigatórias faltando: {missing_columns}",
                    "columns_found": list(df_raw.columns),
                    "columns_required": required_columns,
                    "tip": "Certifique-se de que o arquivo tem as colunas: Data_Venda, Produto, Preco_Local, Quantidade"
                }
            )
        
        validation_extract = validate_sales_data(df_raw)
        
        # TRANSFORM
        df_clean = clean_data(df_raw)
        df_standardized = standardize_data(df_clean)
        
        # Enriquecimento
        if exchange_rate is None:
            exchange_data = extract_exchange_rate_api()
        else:
            exchange_data = {'rate': exchange_rate, 'source': 'custom'}
        
        df_enriched = enrich_data(df_standardized, exchange_data)
        df_aggregated = aggregate_data(df_enriched)
        
        validation_transform = validate_transformed_data(df_enriched)
        
        # LOAD
        load_status = None
        if load_to_db:
            engine = create_database_connection()
            load_to_database(df_enriched, detailed_table, engine, load_mode='replace')
            load_to_database(df_aggregated, aggregated_table, engine, load_mode='replace')
            load_status = "success"
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        return JSONResponse(content={
            "status": "success",
            "message": "Pipeline ETL executado com sucesso",
            "execution": {
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "duration_seconds": execution_time
            },
            "extract": {
                "source_file": file.filename,
                "records_extracted": len(df_raw),
                "validation": validation_extract
            },
            "transform": {
                "records_detailed": len(df_enriched),
                "records_aggregated": len(df_aggregated),
                "exchange_rate": exchange_data.get('rate'),
                "validation": validation_transform
            },
            "load": {
                "status": load_status,
                "detailed_table": detailed_table if load_to_db else None,
                "aggregated_table": aggregated_table if load_to_db else None
            },
            "detailed_data": dataframe_to_dict(df_enriched, max_rows=10),
            "aggregated_data": dataframe_to_dict(df_aggregated, max_rows=10)
        })
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no pipeline: {str(e)}")


# ============================================================
# EXECUÇÃO
# ============================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
