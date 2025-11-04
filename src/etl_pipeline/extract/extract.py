"""
Módulo de Extração (Extract) - Fase E do ETL
Responsável por extrair dados de múltiplas fontes:
1. Arquivo CSV local (vendas)
2. APIs públicas (cotações de moeda e criptomoeda)
"""

import pandas as pd
import requests
import os
from datetime import datetime
import logging
from typing import Optional, Dict, Any

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def validate_api_response(data: Dict[Any, Any], expected_keys: list, source: str) -> bool:
    """
    Valida se a resposta da API contém os campos esperados.
    
    Args:
        data: Dicionário com resposta da API
        expected_keys: Lista de chaves obrigatórias
        source: Nome da fonte para logging
    
    Returns:
        True se válido, False caso contrário
    """
    if not isinstance(data, dict):
        logger.warning(f"  ⚠️ {source}: Resposta não é um dicionário válido")
        return False
    
    missing_keys = [key for key in expected_keys if key not in data]
    if missing_keys:
        logger.warning(f"  ⚠️ {source}: Campos faltando na resposta: {missing_keys}")
        return False
    
    return True


def validate_crypto_price(price: float, crypto: str = 'BTC') -> bool:
    """
    Valida se o preço da criptomoeda está em uma faixa razoável.
    
    Args:
        price: Preço a validar
        crypto: Símbolo da criptomoeda
    
    Returns:
        True se válido, False caso contrário
    """
    # Validação básica: BTC entre $10k e $500k (range amplo para segurança)
    if crypto == 'BTC':
        if not (10000 <= price <= 500000):
            logger.warning(f"  ⚠️ Preço suspeito para {crypto}: ${price:,.2f}")
            return False
    
    return True


def validate_exchange_rate(rate: float, base: str, target: str) -> bool:
    """
    Valida se a taxa de câmbio está em uma faixa razoável.
    
    Args:
        rate: Taxa de câmbio
        base: Moeda base
        target: Moeda alvo
    
    Returns:
        True se válido, False caso contrário
    """
    # Validação básica: BRL→USD entre 0.10 e 0.50 (range amplo)
    if base == 'BRL' and target == 'USD':
        if not (0.10 <= rate <= 0.50):
            logger.warning(f"  ⚠️ Taxa de câmbio suspeita: 1 {base} = {rate} {target}")
            return False
    
    return True


def extract_csv_data(file_path: str = None) -> pd.DataFrame:
    """
    Extrai dados de vendas de um arquivo CSV local.
    
    Args:
        file_path: Caminho do arquivo CSV. Se None, usa o caminho padrão.
    
    Returns:
        DataFrame com os dados de vendas
    
    Raises:
        FileNotFoundError: Se o arquivo não existir
    """
    if file_path is None:
        # Caminho padrão relativo ao projeto (busca na raiz)
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        file_path = os.path.join(base_dir, 'data', 'raw', 'vendas.csv')
    
    try:
        logger.info(f"📂 Extraindo dados do CSV: {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"✅ {len(df)} registros extraídos do CSV")
        return df
    except FileNotFoundError:
        logger.error(f"❌ Arquivo não encontrado: {file_path}")
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao ler CSV: {e}")
        raise


def extract_exchange_rate_api(base_currency: str = 'BRL', target_currency: str = 'USD') -> dict:
    """
    Extrai taxa de câmbio usando múltiplas APIs confiáveis.
    
    Implementa sistema de failover com 3 APIs principais:
    1. Frankfurter (Primária) - API gratuita do Banco Central Europeu
    2. ExchangeRate-API (Secundária) - API confiável com dados atualizados
    3. FreeCurrencyAPI (Terciária) - Backup adicional
    
    Args:
        base_currency: Moeda base (padrão: BRL - Real Brasileiro)
        target_currency: Moeda alvo (padrão: USD - Dólar)
    
    Returns:
        Dicionário com a taxa de conversão e metadados
    
    Raises:
        Exception: Se todas as APIs falharem
    """
    logger.info(f"🌐 Extraindo taxa de câmbio: {base_currency} → {target_currency}")
    
    # Lista de APIs em ordem de prioridade
    apis = [
        {
            'name': 'Frankfurter',
            'url': f"https://api.frankfurter.app/latest?from={base_currency}&to={target_currency}",
            'parser': 'frankfurter'
        },
        {
            'name': 'ExchangeRate-API',
            'url': f"https://open.er-api.com/v6/latest/{base_currency}",
            'parser': 'exchangerate'
        },
        {
            'name': 'Fixer.io (Fallback)',
            'url': f"https://api.fixer.io/latest?base={base_currency}&symbols={target_currency}",
            'parser': 'fixer'
        }
    ]
    
    # Tenta cada API em sequência
    for api in apis:
        try:
            logger.info(f"  📡 Tentando API: {api['name']}...")
            
            response = requests.get(
                api['url'],
                timeout=8,
                headers={'User-Agent': 'ETL-Pipeline/1.0'}
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Parse baseado no tipo de API
            result = _parse_exchange_rate_response(
                data, 
                api['parser'], 
                base_currency, 
                target_currency,
                api['name']
            )
            
            if result and result.get('rate', 0) > 0:
                logger.info(f"✅ Taxa de câmbio via {api['name']}: 1 {base_currency} = {result['rate']} {target_currency}")
                return result
        
        except requests.exceptions.Timeout:
            logger.warning(f"  ⏱️ Timeout na API {api['name']} - tentando próxima...")
            continue
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"  ⚠️ Erro na API {api['name']}: {str(e)[:100]} - tentando próxima...")
            continue
        
        except Exception as e:
            logger.warning(f"  ⚠️ Erro ao processar resposta da {api['name']}: {str(e)[:100]}")
            continue
    
    # Se todas as APIs falharam
    logger.error("❌ ERRO CRÍTICO: Todas as APIs de câmbio falharam!")
    raise Exception(
        f"Não foi possível obter taxa de câmbio {base_currency}→{target_currency} de nenhuma fonte. "
        "Verifique sua conexão com a internet e tente novamente."
    )


def _parse_exchange_rate_response(data: dict, parser_type: str, base: str, target: str, source: str) -> dict:
    """
    Parseia a resposta de diferentes APIs de câmbio.
    
    Args:
        data: Resposta JSON da API
        parser_type: Tipo de parser ('frankfurter', 'exchangerate', 'fixer')
        base: Moeda base
        target: Moeda alvo
        source: Nome da fonte dos dados
    
    Returns:
        Dicionário padronizado com taxa de câmbio ou None se falhar
    """
    try:
        if parser_type == 'frankfurter':
            return {
                'base': base,
                'target': target,
                'rate': data['rates'][target],
                'date': data['date'],
                'source': source,
                'timestamp': datetime.now().isoformat(),
                'fallback': False
            }
        
        elif parser_type == 'exchangerate':
            return {
                'base': base,
                'target': target,
                'rate': data['rates'].get(target, 0),
                'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                'source': source,
                'timestamp': datetime.now().isoformat(),
                'fallback': False
            }
        
        elif parser_type == 'fixer':
            return {
                'base': base,
                'target': target,
                'rate': data['rates'].get(target, 0),
                'date': data.get('date', datetime.now().strftime('%Y-%m-%d')),
                'source': source,
                'timestamp': datetime.now().isoformat(),
                'fallback': False
            }
        
        return None
    
    except (KeyError, ValueError, TypeError) as e:
        logger.warning(f"  ⚠️ Erro ao parsear resposta de câmbio: {e}")
        return None


def extract_crypto_price_api(crypto: str = 'BTC') -> dict:
    """
    Extrai cotação atual de criptomoeda usando múltiplas APIs confiáveis.
    
    Implementa sistema de failover com 4 APIs principais:
    1. CoinGecko API (Primária) - Mais confiável e completa
    2. Binance API (Secundária) - Alta disponibilidade
    3. CoinCap API (Terciária) - Dados em tempo real
    4. CoinDesk API (Quaternária) - Backup final
    
    Args:
        crypto: Código da criptomoeda (padrão: BTC - Bitcoin)
    
    Returns:
        Dicionário com a cotação em diferentes moedas e fonte dos dados
    
    Raises:
        Exception: Se todas as APIs falharem
    """
    logger.info(f"🪙 Extraindo cotação de {crypto} com sistema multi-API...")
    
    # Lista de APIs em ordem de prioridade
    apis = [
        {
            'name': 'CoinGecko',
            'url': 'https://api.coingecko.com/api/v3/simple/price',
            'params': {
                'ids': 'bitcoin',
                'vs_currencies': 'usd,eur,gbp,brl'
            },
            'parser': 'coingecko'
        },
        {
            'name': 'Binance',
            'url': 'https://api.binance.com/api/v3/ticker/price',
            'params': {'symbol': 'BTCUSDT'},
            'parser': 'binance'
        },
        {
            'name': 'CoinCap',
            'url': 'https://api.coincap.io/v2/assets/bitcoin',
            'params': {},
            'parser': 'coincap'
        },
        {
            'name': 'CoinDesk',
            'url': 'https://api.coindesk.com/v1/bpi/currentprice.json',
            'params': {},
            'parser': 'coindesk'
        }
    ]
    
    # Tenta cada API em sequência
    for api in apis:
        try:
            logger.info(f"  📡 Tentando API: {api['name']}...")
            
            response = requests.get(
                api['url'],
                params=api['params'] if api['params'] else None,
                timeout=8,
                headers={'User-Agent': 'ETL-Pipeline/1.0'}
            )
            response.raise_for_status()
            
            data = response.json()
            
            # Parse baseado no tipo de API
            result = _parse_crypto_response(data, api['parser'], crypto, api['name'])
            
            if result:
                logger.info(f"✅ Cotação {crypto} obtida via {api['name']}: ${result['usd_price']:,.2f} USD")
                logger.info(f"   💰 EUR: €{result.get('eur_price', 0):,.2f} | GBP: £{result.get('gbp_price', 0):,.2f} | BRL: R${result.get('brl_price', 0):,.2f}")
                return result
        
        except requests.exceptions.Timeout:
            logger.warning(f"  ⏱️ Timeout na API {api['name']} - tentando próxima...")
            continue
        
        except requests.exceptions.RequestException as e:
            logger.warning(f"  ⚠️ Erro na API {api['name']}: {str(e)[:100]} - tentando próxima...")
            continue
        
        except Exception as e:
            logger.warning(f"  ⚠️ Erro ao processar resposta da {api['name']}: {str(e)[:100]}")
            continue
    
    # Se todas as APIs falharam
    logger.error("❌ ERRO CRÍTICO: Todas as APIs de criptomoeda falharam!")
    raise Exception(
        "Não foi possível obter cotação de criptomoeda de nenhuma fonte. "
        "Verifique sua conexão com a internet e tente novamente."
    )


def _parse_crypto_response(data: dict, parser_type: str, crypto: str, source: str) -> dict:
    """
    Parseia a resposta de diferentes APIs de criptomoeda.
    
    Args:
        data: Resposta JSON da API
        parser_type: Tipo de parser ('coingecko', 'binance', 'coincap', 'coindesk')
        crypto: Símbolo da criptomoeda
        source: Nome da fonte dos dados
    
    Returns:
        Dicionário padronizado com cotações ou None se falhar
    """
    try:
        if parser_type == 'coingecko':
            btc_data = data.get('bitcoin', {})
            return {
                'crypto': crypto,
                'usd_price': btc_data.get('usd', 0),
                'eur_price': btc_data.get('eur', 0),
                'gbp_price': btc_data.get('gbp', 0),
                'brl_price': btc_data.get('brl', 0),
                'source': source,
                'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                'timestamp': datetime.now().isoformat(),
                'fallback': False
            }
        
        elif parser_type == 'binance':
            usd_price = float(data.get('price', 0))
            return {
                'crypto': crypto,
                'usd_price': usd_price,
                'eur_price': usd_price * 0.92,  # Conversão aproximada
                'gbp_price': usd_price * 0.79,  # Conversão aproximada
                'brl_price': usd_price * 5.35,  # Conversão aproximada
                'source': source,
                'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                'timestamp': datetime.now().isoformat(),
                'fallback': False,
                'note': 'Conversões EUR/GBP/BRL são aproximadas'
            }
        
        elif parser_type == 'coincap':
            usd_price = float(data.get('data', {}).get('priceUsd', 0))
            return {
                'crypto': crypto,
                'usd_price': usd_price,
                'eur_price': usd_price * 0.92,  # Conversão aproximada
                'gbp_price': usd_price * 0.79,  # Conversão aproximada
                'brl_price': usd_price * 5.35,  # Conversão aproximada
                'source': source,
                'updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
                'timestamp': datetime.now().isoformat(),
                'fallback': False,
                'note': 'Conversões EUR/GBP/BRL são aproximadas'
            }
        
        elif parser_type == 'coindesk':
            return {
                'crypto': crypto,
                'usd_price': data['bpi']['USD']['rate_float'],
                'eur_price': data['bpi']['EUR']['rate_float'],
                'gbp_price': data['bpi']['GBP']['rate_float'],
                'brl_price': data['bpi']['USD']['rate_float'] * 5.35,  # Aproximado
                'source': source,
                'updated': data['time']['updated'],
                'timestamp': datetime.now().isoformat(),
                'fallback': False
            }
        
        return None
    
    except (KeyError, ValueError, TypeError) as e:
        logger.warning(f"  ⚠️ Erro ao parsear resposta: {e}")
        return None


def extract_all_sources() -> tuple:
    """
    Função principal que extrai dados de todas as fontes.
    
    Returns:
        Tupla contendo (vendas_df, exchange_rate_dict, crypto_dict)
    """
    logger.info("🚀 Iniciando extração de todas as fontes...")
    
    # Extração de dados locais (CSV)
    vendas_df = extract_csv_data()
    
    # Extração de dados externos (APIs)
    exchange_rate = extract_exchange_rate_api()
    crypto_price = extract_crypto_price_api()
    
    logger.info("✅ Todas as extrações concluídas com sucesso!")
    
    return vendas_df, exchange_rate, crypto_price


# Para teste standalone
if __name__ == "__main__":
    print("=" * 60)
    print("TESTE DO MÓDULO DE EXTRAÇÃO")
    print("=" * 60)
    
    try:
        vendas, taxa, cripto = extract_all_sources()
        
        print("\n📊 RESUMO DA EXTRAÇÃO:")
        print(f"- Vendas: {len(vendas)} registros")
        print(f"- Taxa de Câmbio: 1 {taxa['base']} = {taxa['rate']} {taxa['target']}")
        print(f"- Bitcoin: ${cripto['usd_price']:,.2f} USD")
        
        print("\n🔍 Primeiras linhas do CSV:")
        print(vendas.head())
        
    except Exception as e:
        logger.error(f"Erro no teste: {e}")
