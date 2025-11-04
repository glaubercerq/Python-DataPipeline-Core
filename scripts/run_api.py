"""
Script para iniciar a API REST do Pipeline ETL
Executa o servidor Uvicorn com hot-reload
"""

import uvicorn
import sys
from pathlib import Path

# Adiciona o diretório raiz ao path
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))


if __name__ == "__main__":
    print("🚀 Iniciando API REST - Pipeline ETL")
    print("=" * 70)
    print("📍 Swagger UI: http://127.0.0.1:8000/docs")
    print("📍 ReDoc: http://127.0.0.1:8000/redoc")
    print("📍 Health Check: http://127.0.0.1:8000/health")
    print("=" * 70)
    print()
    
    uvicorn.run(
        "src.etl_pipeline.api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
