-- ================================================
-- EXEMPLOS DE CONSULTAS SQL - Sales Data Warehouse
-- ================================================

-- 1. Top 5 dias com maior faturamento
SELECT 
    Data_Venda,
    Total_Vendas_USD,
    Numero_Transacoes,
    Ticket_Medio_USD,
    Produtos_Unicos
FROM vendas_agregadas
ORDER BY Total_Vendas_USD DESC
LIMIT 5;

-- 2. Análise de vendas por categoria
SELECT 
    Categoria,
    COUNT(*) as Total_Transacoes,
    SUM(Quantidade) as Total_Itens_Vendidos,
    ROUND(SUM(Valor_Total_USD), 2) as Faturamento_Total_USD,
    ROUND(AVG(Preco_USD), 2) as Preco_Medio_USD
FROM vendas_detalhadas
GROUP BY Categoria
ORDER BY Faturamento_Total_USD DESC;

-- 3. Vendas por região
SELECT 
    Regiao,
    COUNT(*) as Numero_Vendas,
    ROUND(SUM(Valor_Total_USD), 2) as Total_Faturamento_USD,
    ROUND(AVG(Valor_Total_USD), 2) as Ticket_Medio_USD
FROM vendas_detalhadas
GROUP BY Regiao
ORDER BY Total_Faturamento_USD DESC;

-- 4. Top 10 produtos mais vendidos
SELECT 
    Produto,
    SUM(Quantidade) as Quantidade_Total,
    COUNT(*) as Numero_Vendas,
    ROUND(SUM(Valor_Total_USD), 2) as Receita_Total_USD
FROM vendas_detalhadas
GROUP BY Produto
ORDER BY Receita_Total_USD DESC
LIMIT 10;

-- 5. Análise temporal - Vendas por dia da semana
SELECT 
    Nome_Dia,
    Dia_Semana,
    COUNT(*) as Total_Vendas,
    ROUND(AVG(Valor_Total_USD), 2) as Ticket_Medio_USD
FROM vendas_detalhadas
GROUP BY Nome_Dia, Dia_Semana
ORDER BY Dia_Semana;

-- 6. Distribuição por categoria de valor
SELECT 
    Categoria_Valor,
    COUNT(*) as Quantidade_Vendas,
    ROUND(SUM(Valor_Total_USD), 2) as Total_USD
FROM vendas_detalhadas
GROUP BY Categoria_Valor
ORDER BY 
    CASE Categoria_Valor
        WHEN 'Baixo' THEN 1
        WHEN 'Médio' THEN 2
        WHEN 'Alto' THEN 3
        WHEN 'Premium' THEN 4
    END;

-- 7. Evolução temporal de vendas
SELECT 
    Data_Venda,
    Total_Vendas_USD,
    Numero_Transacoes,
    ROUND(Ticket_Medio_USD, 2) as Ticket_Medio
FROM vendas_agregadas
ORDER BY Data_Venda;

-- 8. Produtos eletrônicos com maior faturamento
SELECT 
    Produto,
    Categoria,
    SUM(Quantidade) as Qtd_Vendida,
    ROUND(SUM(Valor_Total_USD), 2) as Receita_USD
FROM vendas_detalhadas
WHERE Categoria = 'eletrônicos'
GROUP BY Produto, Categoria
ORDER BY Receita_USD DESC;

-- 9. Análise comparativa de preços
SELECT 
    Produto,
    ROUND(MIN(Preco_USD), 2) as Preco_Min_USD,
    ROUND(MAX(Preco_USD), 2) as Preco_Max_USD,
    ROUND(AVG(Preco_USD), 2) as Preco_Medio_USD
FROM vendas_detalhadas
GROUP BY Produto
HAVING COUNT(*) > 1
ORDER BY Preco_Medio_USD DESC;

-- 10. Histórico de execuções do pipeline ETL
SELECT 
    execution_id,
    execution_date,
    pipeline_status,
    records_processed,
    tables_loaded,
    ROUND(execution_time_seconds, 2) as tempo_execucao_seg,
    notes
FROM etl_metadata
ORDER BY execution_id DESC;

-- 11. KPIs principais do negócio
SELECT 
    COUNT(DISTINCT Data_Venda) as Total_Dias_Ativos,
    COUNT(*) as Total_Transacoes,
    SUM(Quantidade) as Total_Itens_Vendidos,
    ROUND(SUM(Valor_Total_USD), 2) as Receita_Total_USD,
    ROUND(AVG(Valor_Total_USD), 2) as Ticket_Medio_USD,
    COUNT(DISTINCT Produto) as Produtos_Diferentes,
    COUNT(DISTINCT Regiao) as Regioes_Atendidas
FROM vendas_detalhadas;

-- 12. Análise de acessórios vs eletrônicos
SELECT 
    CASE 
        WHEN Categoria IN ('eletrônicos') THEN 'Eletrônicos'
        WHEN Categoria IN ('acessórios') THEN 'Acessórios'
        ELSE 'Outros'
    END as Grupo_Categoria,
    COUNT(*) as Total_Vendas,
    ROUND(SUM(Valor_Total_USD), 2) as Faturamento_USD,
    ROUND(AVG(Valor_Total_USD), 2) as Ticket_Medio_USD
FROM vendas_detalhadas
GROUP BY Grupo_Categoria
ORDER BY Faturamento_USD DESC;
