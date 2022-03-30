USE db_flight;
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

-- descricao: aeroportos mais movimentados
DROP VIEW IF EXISTS vw_aeroporto_voo;
CREATE VIEW vw_aeroporto_voo
PARTITIONED ON (data_ano, data_mes)
AS 
SELECT 
    cod_aeroporto
    ,sum(qtd_total_voo) soma_voo
    ,dense_rank() OVER(PARTITION BY data_ano, data_mes ORDER BY sum(qtd_total_voo) DESC) rank_voo
    ,data_ano, data_mes
FROM tb_aeroporto_companhia
GROUP BY cod_aeroporto, data_ano, data_mes
ORDER BY data_ano DESC, data_mes DESC, rank_voo ASC;

-- descricao: companhias aereas que mais atrasam
DROP VIEW IF EXISTS vw_atraso_companhia;
CREATE VIEW vw_atraso_companhia
PARTITIONED ON (ano, mes)
AS
WITH cte_atraso AS (
    SELECT 
    companhia, ano, mes
    ,coalesce(atraso_partida,0) + coalesce(atraso_chegada,0) AS total_atraso
    ,atraso_partida
    ,atraso_chegada
    FROM tb_voo
)
SELECT 
    companhia
    ,avg(total_atraso) AS media_atraso
    ,ano, mes
FROM cte_atraso
GROUP BY companhia, ano, mes
ORDER BY ano, mes;

-- descricao: companhias aereas que mais voam
DROP VIEW IF EXISTS vw_companhia_voo;
CREATE VIEW vw_companhia_voo
PARTITIONED ON (ano, mes)
AS 
SELECT 
    companhia
    ,count(*) AS qtd_voo
    ,dense_rank() OVER(PARTITION BY ano,mes ORDER BY count(*) DESC) AS rank_voo
    ,ano,mes
FROM tb_voo
GROUP BY companhia, ano, mes
ORDER BY ano DESC, mes DESC, rank_voo DESC;

-- descricao: companhias aereas que mais atrasam por faixa
DROP VIEW IF EXISTS vw_faixa_atraso;
CREATE VIEW vw_faixa_atraso
PARTITIONED ON (ano,mes)
AS 
WITH cte_atraso AS (
    SELECT 
        companhia, ano, mes
        ,avg(coalesce(atraso_partida,0)+coalesce(atraso_chegada,0)) AS media_atraso
    FROM tb_voo
    GROUP BY companhia, ano, mes
)
SELECT 
    companhia,media_atraso
    ,CASE
        WHEN media_atraso BETWEEN 0 AND 31 THEN '0 < 30 min'
        WHEN media_atraso BETWEEN 0 AND 31 THEN '31 - 60 min'
        WHEN media_atraso BETWEEN 0 AND 31 THEN '61 - 90 min'
        WHEN media_atraso > 90 THEN '> 90 min'
        ELSE 'sem atraso'
    END faixa_atraso
    ,ano,mes
FROM cte_atraso
ORDER BY ano DESC, mes DESC;

-- descricao: principais motivos de cancelamento
DROP VIEW IF EXISTS vw_motivo_cancelamento;
CREATE VIEW vw_motivo_cancelamento
PARTITIONED ON (ano, mes)
AS 
SELECT 
CASE 
    WHEN motivo_cancelamento = 'A' THEN 'Companhia/Operadora'
    WHEN motivo_cancelamento = 'B' THEN 'Clima'
    WHEN motivo_cancelamento = 'C' THEN 'Sistema Nacional de Ar'
    WHEN motivo_cancelamento = 'D' THEN 'Seguranca'
    ELSE 'N/A'
END desc_motivo_cancelamento,
count(motivo_cancelamento) AS qtd_motivo_cancelamento,
dense_rank() OVER(PARTITION BY ano, mes ORDER BY count(motivo_cancelamento) DESC) AS rank_cancelamento,
ano, mes
FROM tb_voo
WHERE cancelado = 1
GROUP BY motivo_cancelamento, ano, mes
ORDER BY ano DESC, mes DESC, rank_cancelamento ASC;

-- descricao: qual dia da semana e mais movimentado
DROP VIEW IF EXISTS vw_movimentacao_diaria;
CREATE VIEW vw_movimentacao_diaria
PARTITIONED ON (ano, mes)
AS 
SELECT 
    dia_semana, 
    count(dia_semana) AS total_cancelado,
    dense_rank() OVER(PARTITION BY ano, mes ORDER BY count(dia_semana) DESC) AS rank_dia_semana,
    ano, mes
FROM tb_voo 
GROUP BY dia_semana, ano, mes
ORDER BY ano DESC, mes DESC, rank_dia_semana ASC;

-- descricao: quanto tempo de voo e qual distancia percorrida por cada companhia
DROP VIEW IF EXISTS vw_tempo_distancia_voo;
CREATE VIEW vw_tempo_distancia_voo
PARTITIONED ON (ano, mes)
AS 
SELECT 
    companhia,
    sum(distancia) AS soma_distancia,
    dense_rank() OVER(PARTITION BY ano, mes ORDER BY SUM(distancia) DESC) AS rank_distancia,
    sum(tempo_ar) AS soma_tempo_ar,
    dense_rank() OVER(PARTITION BY ano, mes ORDER BY SUM(tempo_ar) DESC) AS rank_tempo_ar,
    ano, mes
FROM tb_voo
GROUP BY companhia, ano, mes
ORDER BY ano DESC, mes DESC, soma_tempo_ar DESC, soma_distancia DESC;