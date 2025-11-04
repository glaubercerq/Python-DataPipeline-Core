-- ================================================
-- EXEMPLOS DE CONSULTAS SQL - Sales Data Warehouse
-- ================================================
-- Estrutura das Tabelas no Banco:
-- 
-- 1. vendas_detalhadas (15 colunas):
--    - Data_Venda, Produto, Categoria, Quantidade, Preco_Local
--    - Regiao, Taxa_Cambio, Preco_USD, Valor_Total_USD
--    - Ano, Mes, Dia_Semana, Nome_Dia, Categoria_Valor, Data_Processamento
--
-- 2. vendas_agregadas (8 colunas):
--    - Data_Venda, Total_Vendas_USD, Ticket_Medio_USD, Numero_Transacoes
--    - Quantidade_Total, Preco_Medio_USD, Produtos_Unicos, Itens_Por_Transacao
--
-- 3. etl_metadata (7 colunas):
--    - execution_id, execution_date, pipeline_status, records_processed
--    - tables_loaded, execution_time_seconds, notes
-- ================================================

-- 1. Top 5 dias com maior faturamento
SELECT
    "Data_Venda",
    "Total_Vendas_USD",
    "Numero_Transacoes",
    "Ticket_Medio_USD",
    "Produtos_Unicos"
FROM vendas_agregadas
ORDER BY "Total_Vendas_USD" DESC
LIMIT 5;

-- 2. Análise de vendas por categoria
SELECT
    "Categoria",
    COUNT(*) as Total_Transacoes,
    SUM("Quantidade") as Total_Itens_Vendidos,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Faturamento_Total_USD,
    ROUND(CAST(AVG("Preco_USD") AS numeric), 2) as Preco_Medio_USD
FROM vendas_detalhadas
GROUP BY "Categoria"
ORDER BY Faturamento_Total_USD DESC;

-- 3. Vendas por região
SELECT
    "Regiao",
    COUNT(*) as Numero_Vendas,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Total_Faturamento_USD,
    ROUND(CAST(AVG("Valor_Total_USD") AS numeric), 2) as Ticket_Medio_USD
FROM vendas_detalhadas
GROUP BY "Regiao"
ORDER BY Total_Faturamento_USD DESC;

-- 4. Top 10 produtos mais vendidos
SELECT
    "Produto",
    SUM("Quantidade") as Quantidade_Total,
    COUNT(*) as Numero_Vendas,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Receita_Total_USD
FROM vendas_detalhadas
GROUP BY "Produto"
ORDER BY Receita_Total_USD DESC
LIMIT 10;

-- 5. Análise temporal - Vendas por dia da semana
SELECT
    "Nome_Dia",
    "Dia_Semana",
    COUNT(*) as Total_Vendas,
    ROUND(CAST(AVG("Valor_Total_USD") AS numeric), 2) as Ticket_Medio_USD
FROM vendas_detalhadas
GROUP BY "Nome_Dia", "Dia_Semana"
ORDER BY "Dia_Semana";

-- 6. Distribuição por categoria de valor
SELECT
    "Categoria_Valor",
    COUNT(*) as Total_Vendas,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Faturamento_USD,
    ROUND(CAST(AVG("Valor_Total_USD") AS numeric), 2) as Ticket_Medio_USD
FROM vendas_detalhadas
GROUP BY "Categoria_Valor"
ORDER BY Faturamento_USD DESC;

-- 7. Evolução temporal de vendas (por mês)
SELECT
    "Ano",
    "Mes",
    COUNT(*) as Total_Vendas,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Receita_Mensal_USD,
    ROUND(CAST(AVG("Valor_Total_USD") AS numeric), 2) as Ticket_Medio_Mensal_USD
FROM vendas_detalhadas
GROUP BY "Ano", "Mes"
ORDER BY "Ano", "Mes";

-- 8. Produtos com maior margem (preço médio > 100 USD)
SELECT
    "Produto",
    ROUND(CAST(AVG("Preco_USD") AS numeric), 2) as Preco_Medio_USD,
    COUNT(*) as Numero_Vendas,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Receita_Total_USD
FROM vendas_detalhadas
GROUP BY "Produto"
HAVING AVG("Preco_USD") > 100
ORDER BY Preco_Medio_USD DESC;

-- 9. Análise de regiões por categoria
SELECT
    "Regiao",
    "Categoria",
    COUNT(*) as Numero_Vendas,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Faturamento_USD
FROM vendas_detalhadas
GROUP BY "Regiao", "Categoria"
ORDER BY "Regiao", Faturamento_USD DESC;

-- 10. Histórico de execuções do pipeline ETL
SELECT
    execution_id,
    execution_date,
    pipeline_status,
    records_processed,
    tables_loaded,
    ROUND(CAST(execution_time_seconds AS numeric), 2) as tempo_execucao_seg,
    notes
FROM etl_metadata
ORDER BY execution_id DESC;

-- 11. KPIs principais do negócio
SELECT
    COUNT(DISTINCT CAST("Data_Venda" AS DATE)) as Total_Dias_Ativos,
    COUNT(*) as Total_Transacoes,
    SUM("Quantidade") as Total_Itens_Vendidos,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Receita_Total_USD,
    ROUND(CAST(AVG("Valor_Total_USD") AS numeric), 2) as Ticket_Medio_USD,
    COUNT(DISTINCT "Produto") as Produtos_Diferentes,
    COUNT(DISTINCT "Regiao") as Regioes_Atendidas
FROM vendas_detalhadas;

-- 12. Análise de acessórios vs eletrônicos
SELECT
    CASE
        WHEN "Categoria" = 'eletrônicos' THEN 'Eletrônicos'
        WHEN "Categoria" = 'acessórios' THEN 'Acessórios'
        ELSE 'Outros'
    END as Grupo_Categoria,
    COUNT(*) as Total_Vendas,
    ROUND(CAST(SUM("Valor_Total_USD") AS numeric), 2) as Faturamento_USD,
    ROUND(CAST(AVG("Valor_Total_USD") AS numeric), 2) as Ticket_Medio_USD
FROM vendas_detalhadas
GROUP BY Grupo_Categoria
ORDER BY Faturamento_USD DESC;