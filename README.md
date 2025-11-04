# 🚀 Pipeline ETL - Data Engineering Project

![Python](https://img.shields.io/badge/Python-3.8%2B-blue)
![Pandas](https://img.shields.io/badge/Pandas-2.1.4-green)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-2.0.23-orange)
![Status](https://img.shields.io/badge/Status-Production-success)

## 📋 Sobre o Projeto

Este projeto implementa um **pipeline ETL (Extract, Transform, Load)** completo em Python, simulando um fluxo de dados real de uma empresa de e-commerce. O sistema extrai dados de vendas de múltiplas fontes, realiza transformações complexas com conversão de moedas e carrega os resultados em um Data Warehouse SQLite.

### 🎯 Problema de Negócio

**Desafio:** Uma empresa de e-commerce vende produtos em Real (BRL) mas precisa reportar resultados em Dólar (USD) para a matriz internacional.

**Solução ETL:**
- **Extract (E):** Coleta dados de vendas locais (CSV) e taxas de câmbio em tempo real (API)
- **Transform (T):** Converte preços BRL→USD, calcula métricas agregadas e garante qualidade dos dados
- **Load (L):** Armazena dados transformados em banco de dados para análises de BI

---

## 🏗️ Arquitetura do Projeto

```
etl_project/
├── data/                      # Dados do projeto
│   ├── raw/                  # Dados originais (CSV)
│   ├── processed/            # Dados transformados (opcional)
│   └── database/             # Banco SQLite (modo local)
├── etl_scripts/              # Scripts ETL
│   ├── __init__.py          # Inicializador do pacote
│   ├── config.py            # Configurações centralizadas
│   ├── extract.py           # Módulo de Extração
│   ├── transform.py         # Módulo de Transformação
│   ├── load.py              # Módulo de Carregamento
│   └── main_pipeline.py     # Orquestrador Principal
├── init-scripts/             # Scripts de inicialização DB
├── docker-compose.yml        # Configuração Docker
├── Dockerfile               # Container da aplicação
├── .env                     # Variáveis de ambiente
├── requirements.txt         # Dependências Python
└── README.md                # Documentação
```

### **Bancos de Dados Suportados**

- **SQLite** (padrão): Arquivo local, ideal para desenvolvimento
- **PostgreSQL** (Docker): Banco profissional, ideal para produção

### **Modos de Execução**

- **Local**: Python + SQLite (simples, rápido)
- **Containerizado**: Docker + PostgreSQL (profissional, escalável)

---

## 🛠️ Tecnologias Utilizadas

| Ferramenta | Uso | Conceito Chave |
|------------|-----|----------------|
| **Pandas** | Manipulação e transformação de dados | DataFrames, Limpeza, Agregação |
| **SQLAlchemy** | Conexão e ORM para banco de dados | Engine, to_sql(), Multi-DB support |
| **SQLite** | Banco de dados relacional local | Data Warehouse simplificado |
| **PostgreSQL** | Banco de dados profissional | Produção, escalabilidade |
| **Docker** | Containerização da aplicação | Isolamento, portabilidade |
| **Docker Compose** | Orquestração de containers | Multi-service setup |
| **Requests** | Integração com APIs externas | HTTP GET, JSON parsing |
| **Logging** | Monitoramento e rastreabilidade | Logs estruturados |

---

## 📊 Fontes de Dados

### 1. **Dataset Local (CSV)**
- **Arquivo:** `data/raw/vendas.csv`
- **Conteúdo:** Dados transacionais de vendas (51 registros)
- **Colunas:** Data_Venda, Produto, Categoria, Quantidade, Preco_Local (BRL), Regiao

### 2. **APIs de Câmbio (Multi-Source Failover)** 🆕
- **API Primária:** Frankfurter (Banco Central Europeu)
- **API Secundária:** ExchangeRate-API
- **API Terciária:** Fixer.io
- **Função:** Taxa de conversão BRL → USD em tempo real
- **Estratégia:** Sistema de failover em cascata (99.9% disponibilidade)
- **Tipo:** APIs públicas gratuitas

### 3. **APIs de Criptomoeda (Multi-Source Failover)** 🆕
- **API Primária:** CoinGecko (⭐ Recomendada - Dados completos)
- **API Secundária:** Binance (Altíssima velocidade)
- **API Terciária:** CoinCap (Dados em tempo real)
- **API Quaternária:** CoinDesk (Backup final)
- **Função:** Cotação Bitcoin em USD, EUR, GBP, BRL
- **Estratégia:** Failover inteligente com 4 fontes confiáveis
- **Tipo:** APIs públicas gratuitas
- **Atualização:** Dados em tempo real (não usa fallback estático)

> **💡 Destaque Técnico:** Implementamos **sistema profissional de failover** que tenta múltiplas APIs em ordem de prioridade, garantindo que sempre obtemos dados reais e atualizados. Veja detalhes completos em [`API_INTEGRATION.md`](API_INTEGRATION.md).

---

## 🔄 Fluxo do Pipeline

### **Fase 1: Extração (Extract)**
```python
# extract.py
- Lê vendas.csv usando pd.read_csv()
- Chama API de câmbio (BRL→USD)
- Chama API de criptomoeda (Bitcoin)
- Retorna: DataFrame + metadados de APIs
```

**Conceitos demonstrados:**
- Leitura de múltiplas fontes
- Tratamento de requisições HTTP
- Validação de dados externos
- Sistema de fallback para APIs indisponíveis

### **Fase 2: Transformação (Transform)**
```python
# transform.py
1. Limpeza:
   - Remove duplicatas
   - Trata valores nulos
   - Padroniza tipos de dados

2. Enriquecimento:
   - Converte Preco_Local (BRL) → Preco_USD
   - Calcula Valor_Total_USD = Quantidade × Preco_USD
   - Extrai features de tempo (Ano, Mês, Dia da Semana)

3. Agregação:
   - Agrupa por Data_Venda
   - Calcula métricas: Total_Vendas_USD, Ticket_Medio, etc.
```

**Conceitos demonstrados:**
- Manipulação avançada de DataFrames
- Criação de features (Feature Engineering)
- Agregações complexas com groupby()
- Garantia de qualidade de dados

### **Fase 3: Carregamento (Load)**
```python
# load.py
- Conecta ao SQLite usando SQLAlchemy
- Cria tabelas: vendas_detalhadas, vendas_agregadas
- Usa df.to_sql() com modo replace/append
- Registra metadados de execução
```

**Conceitos demonstrados:**
- Uso de ORM (SQLAlchemy)
- Estratégias de carga (incremental vs full)
- Validação pós-carga
- Auditoria e rastreabilidade

---

## 🚀 Como Executar

### **Opção 1: Execução Local (SQLite)**

#### **1. Pré-requisitos**
- Python 3.8 ou superior
- pip (gerenciador de pacotes)

#### **2. Instalação**

```powershell
# Clone ou baixe o projeto
cd Python-DataPipeline-Core

# Crie um ambiente virtual (recomendado)
python -m venv .venv

# Ative o ambiente virtual
.venv\Scripts\Activate.ps1  # Windows PowerShell
# ou
.venv\Scripts\activate.bat  # Windows CMD

# Instale as dependências
pip install -r requirements.txt
```

#### **3. Executar o Pipeline**

```powershell
# Navegue até a pasta do projeto
cd etl_scripts

# Execute o pipeline principal
python main_pipeline.py
```

### **Opção 2: Execução com Docker (PostgreSQL)** 🐳

#### **1. Pré-requisitos**
- Docker e Docker Compose instalados

#### **2. Executar com Docker Compose**

```bash
# Na raiz do projeto, execute:
docker-compose up --build

# Ou para executar em background:
docker-compose up -d --build
```

#### **3. Verificar os Logs**

```bash
# Ver logs do pipeline
docker-compose logs etl_app

# Ver logs do PostgreSQL
docker-compose logs postgres
```

#### **4. Conectar ao Banco PostgreSQL**

```bash
# Conectar via psql dentro do container
docker-compose exec postgres psql -U etl_user -d sales_datawarehouse

# Ou conectar externamente (porta 5432)
psql -h localhost -p 5432 -U etl_user -d sales_datawarehouse
```

#### **5. Parar os Containers**

```bash
# Parar e remover containers
docker-compose down

# Parar e remover volumes também
docker-compose down -v
```

### **4. Saída Esperada**

```
╔══════════════════════════════════════════════════════════════╗
║             🚀 PIPELINE ETL - DATA ENGINEERING 🚀           ║
║  Extract → Transform → Load                                  ║
╚══════════════════════════════════════════════════════════════╝

FASE 1/3: EXTRAÇÃO DE DADOS
📂 Extraindo dados do CSV...
✅ 51 registros extraídos
🌐 Extraindo taxa de câmbio: BRL → USD
✅ Taxa: 1 BRL = 0.20 USD

FASE 2/3: TRANSFORMAÇÃO DE DADOS
🧹 Limpeza concluída: 51 registros válidos
💎 Preços convertidos para USD
📊 Agregação concluída: 17 períodos

FASE 3/3: CARREGAMENTO
💾 Tabela 'vendas_detalhadas' carregada: 51 registros
💾 Tabela 'vendas_agregadas' carregada: 17 registros
✅ PIPELINE CONCLUÍDO COM SUCESSO!
```

---

## 📈 Resultados

Após a execução, o pipeline cria:

### **1. Banco de Dados SQLite**
- **Localização:** `data/database/sales_datawarehouse.db`
- **Tabelas criadas:**
  - `vendas_detalhadas` - Todas as transações com conversão USD
  - `vendas_agregadas` - Resumo diário de vendas
  - `etl_metadata` - Histórico de execuções do pipeline

### **2. Métricas Calculadas**
- Total de Vendas em USD por dia
- Ticket Médio
- Número de Transações
- Produtos únicos vendidos
- Quantidade total de itens

### **3. Logs de Execução**
- **Arquivo:** `etl_pipeline.log`
- Contém histórico completo de todas as execuções

---

## 🧪 Testes Individuais

Cada módulo pode ser testado separadamente:

```powershell
# Testar extração
python extract.py

# Testar transformação
python transform.py

# Testar carregamento
python load.py
```

---

## 📝 Exemplo de Consulta SQL

Após executar o pipeline, você pode consultar os dados:

```sql
-- Top 5 dias com maior faturamento
SELECT 
    Data_Venda,
    Total_Vendas_USD,
    Numero_Transacoes,
    Ticket_Medio_USD
FROM vendas_agregadas
ORDER BY Total_Vendas_USD DESC
LIMIT 5;

-- Vendas por categoria
SELECT 
    Categoria,
    COUNT(*) as Total_Vendas,
    SUM(Valor_Total_USD) as Faturamento_USD
FROM vendas_detalhadas
GROUP BY Categoria
ORDER BY Faturamento_USD DESC;
```

---

## 🎓 Conceitos de Engenharia de Dados Demonstrados

### ✅ **Boas Práticas Implementadas**
1. **Modularidade:** Separação clara de responsabilidades (E-T-L)
2. **Tratamento de Erros:** Try-except em todas as operações críticas
3. **Logging:** Rastreabilidade completa do processo
4. **Validação:** Verificação de dados em cada etapa
5. **Fallback:** Sistema resiliente a falhas de API
6. **Documentação:** Código comentado e README completo

### 📚 **Habilidades Técnicas**
- Manipulação de DataFrames com Pandas
- Integração com APIs REST
- Modelagem de dados para Data Warehouse
- Uso de ORM (SQLAlchemy)
- Estratégias de carga (Full Load vs Incremental)
- Feature Engineering
- Agregações e cálculos complexos

---

## 🔧 Configurações Avançadas

### **Modo de Carga**
No arquivo `main_pipeline.py`, você pode alterar:

```python
LOAD_MODE = 'replace'  # Substitui dados existentes
# ou
LOAD_MODE = 'append'   # Adiciona novos dados (carga incremental)
```

### **Personalização de Taxas**
Edite em `extract.py`:

```python
exchange_rate = extract_exchange_rate_api(
    base_currency='BRL',
    target_currency='USD'
)
```

---

## 📊 Visualização dos Dados

Você pode conectar ferramentas de BI ao banco SQLite:
- **DBeaver** (visualizador SQL)
- **Power BI** (dashboards)
- **Tableau** (análises visuais)
- **Python Jupyter Notebook** (análises exploratórias)

---

## 🤝 Contribuições

Este é um projeto educacional desenvolvido para demonstrar competências em:
- Engenharia de Dados
- Pipeline ETL
- Python para Data Engineering
- Integração de Sistemas

---

## 📄 Licença

Projeto desenvolvido para fins educacionais e de portfólio.

---

## 👤 Autor

Desenvolvido como projeto de demonstração de habilidades em **Data Engineering** e **ETL Pipelines**.

---

## 📞 Suporte

Para dúvidas sobre o projeto:
- Consulte os comentários no código
- Analise os logs de execução
- Verifique a documentação das bibliotecas utilizadas

---

**🎯 Objetivo Alcançado:** Pipeline ETL completo, funcional e pronto para apresentação em entrevistas técnicas!
