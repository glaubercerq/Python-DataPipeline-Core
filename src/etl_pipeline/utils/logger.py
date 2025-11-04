"""
Módulo de Logging Estruturado com Loguru
Substitui o logging padrão do Python por Loguru para melhor visualização e rastreamento.

Funcionalidades:
- Logs coloridos no console
- Rotação automática de arquivos (1 arquivo por dia)
- Retenção de 30 dias
- Diferentes níveis (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Formatação estruturada para análise
"""

import sys
from pathlib import Path
from loguru import logger

# Remove configuração padrão do loguru
logger.remove()

# ============================================================
# CONFIGURAÇÃO DE PATHS
# ============================================================

# Diretório raiz do projeto
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent
LOGS_DIR = PROJECT_ROOT / "logs"

# Cria diretório de logs se não existir
LOGS_DIR.mkdir(exist_ok=True)

# ============================================================
# CONFIGURAÇÃO DO CONSOLE (Terminal)
# ============================================================

# Formato colorido para terminal (desenvolvimento)
console_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> | "
    "<level>{message}</level>"
)

logger.add(
    sys.stdout,
    format=console_format,
    level="INFO",
    colorize=True,
    backtrace=True,
    diagnose=True
)

# ============================================================
# CONFIGURAÇÃO DE ARQUIVOS (Produção)
# ============================================================

# Arquivo geral (INFO e acima) - Rotação diária
logger.add(
    LOGS_DIR / "etl_pipeline_{time:YYYY-MM-DD}.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
    level="INFO",
    rotation="00:00",  # Nova arquivo à meia-noite
    retention="30 days",  # Mantém logs por 30 dias
    compression="zip",  # Comprime logs antigos
    encoding="utf-8"
)

# Arquivo de erros (ERROR e CRITICAL) - Rotação por tamanho
logger.add(
    LOGS_DIR / "errors.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} | {message}",
    level="ERROR",
    rotation="10 MB",  # Novo arquivo a cada 10MB
    retention="90 days",  # Mantém erros por 90 dias
    compression="zip",
    backtrace=True,
    diagnose=True,
    encoding="utf-8"
)

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

def log_pipeline_start(pipeline_name: str):
    """Loga o início de um pipeline."""
    logger.info("=" * 80)
    logger.info(f"🚀 INICIANDO PIPELINE: {pipeline_name}")
    logger.info("=" * 80)


def log_pipeline_end(pipeline_name: str, success: bool = True, execution_time: float = None):
    """Loga o fim de um pipeline."""
    status = "✅ SUCESSO" if success else "❌ FALHA"
    logger.info("=" * 80)
    if execution_time:
        logger.info(f"{status} - {pipeline_name} | Tempo: {execution_time:.2f}s")
    else:
        logger.info(f"{status} - {pipeline_name}")
    logger.info("=" * 80)


def log_phase(phase_name: str, step: int = None, total_steps: int = None):
    """Loga uma fase do pipeline."""
    if step and total_steps:
        logger.info(f"\n{'─' * 60}")
        logger.info(f"FASE {step}/{total_steps}: {phase_name}")
        logger.info(f"{'─' * 60}")
    else:
        logger.info(f"\n{'─' * 60}")
        logger.info(f"FASE: {phase_name}")
        logger.info(f"{'─' * 60}")


def log_metric(metric_name: str, value, unit: str = ""):
    """Loga uma métrica importante."""
    logger.info(f"📊 {metric_name}: {value} {unit}".strip())


def log_api_call(api_name: str, success: bool, response_time: float = None):
    """Loga chamadas de API."""
    status = "✅" if success else "❌"
    if response_time:
        logger.info(f"{status} API {api_name} | Tempo: {response_time:.2f}s")
    else:
        logger.info(f"{status} API {api_name}")


# ============================================================
# EXPORTA O LOGGER CONFIGURADO
# ============================================================

__all__ = [
    'logger',
    'log_pipeline_start',
    'log_pipeline_end',
    'log_phase',
    'log_metric',
    'log_api_call'
]
