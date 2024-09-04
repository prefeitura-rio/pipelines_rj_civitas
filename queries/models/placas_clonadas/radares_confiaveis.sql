{{
  config(
    materialized = 'table',
    cluster_by = ['camera_numero']
  )
}}

WITH radar_placa_tipo AS (
    SELECT DISTINCT camera_numero, placa, tipoveiculo
    FROM `rj-cetrio.ocr_radar.readings_2024_*`
    WHERE DATE(datahora) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  -- Limita ao dia anterior
),

-- Apenas 1 radar marcou o veículo como sendo daquele tipo
um_erro AS (
    SELECT placa, tipoveiculo
    FROM radar_placa_tipo
    GROUP BY placa, tipoveiculo
    HAVING COUNT(*) = 1
),

-- Placas vistas por + de 10 radares
placa_muito_vista AS (
    SELECT placa
    FROM radar_placa_tipo
    GROUP BY placa
    HAVING COUNT(*) > 10
),

-- Quantos veículos passaram por essa câmera
veiculos_por_camera AS (
    SELECT camera_numero, COUNT(DISTINCT placa) AS qtd_registros
    FROM radar_placa_tipo
    GROUP BY camera_numero
),

-- Contagem de leituras por placa
placa_counts AS (
    SELECT 
        camera_numero,
        placa,
        COUNT(*) AS placa_count
    FROM `rj-cetrio.ocr_radar.readings_2024_*`
    WHERE DATE(datahora) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  -- Limita ao dia anterior
    GROUP BY camera_numero, placa
),

-- Calcular a mediana ou percentil da contagem de leituras por placa para cada radar
mediana_placa_count_por_radar AS (
    SELECT
        camera_numero,
        APPROX_QUANTILES(placa_count, 100)[OFFSET(50)] AS mediana_placa_count -- mediana
    FROM placa_counts
    GROUP BY camera_numero
),

-- Identificar placas que excedem a mediana
placas_acima_mediana AS (
    SELECT 
        pc.camera_numero,
        pc.placa,
        CASE 
            WHEN pc.placa_count > mpcr.mediana_placa_count THEN 1 
            ELSE 0 
        END AS acima_mediana
    FROM placa_counts pc
    JOIN mediana_placa_count_por_radar mpcr
    ON pc.camera_numero = mpcr.camera_numero
),

-- Contagem de placas acima da mediana por radar
contagem_acima_mediana AS (
    SELECT 
        camera_numero,
        SUM(acima_mediana) AS total_acima_mediana
    FROM placas_acima_mediana
    GROUP BY camera_numero
),

-- Calcular as diferenças de tempo entre leituras consecutivas
diferencas_de_tempo AS (
    SELECT
        camera_numero,
        placa,
        TIMESTAMP_DIFF(datahora, LAG(datahora) OVER (PARTITION BY camera_numero, placa ORDER BY datahora ASC), SECOND) AS diff_seconds
    FROM `rj-cetrio.ocr_radar.readings_2024_*`
    WHERE DATE(datahora) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)  -- Limita ao dia anterior
),

-- Contagem de leituras frequentes por radar
leituras_frequentes_por_radar AS (
    SELECT 
        camera_numero, 
        COUNTIF(diff_seconds <= 60) AS total_leituras_frequentes
    FROM diferencas_de_tempo
    GROUP BY camera_numero
)

SELECT 
    rpt.camera_numero, 
    COUNT(ue.placa) AS qtd_inconsistencias,
    vpc.qtd_registros,
    COALESCE(lfpr.total_leituras_frequentes, 0) AS total_leituras_frequentes,
    1 - (COUNT(ue.placa) / vpc.qtd_registros) AS acuracia,
    1 - (COALESCE(lfpr.total_leituras_frequentes, 0) / vpc.qtd_registros) AS confiabilidade_frequente,
    1 - (COALESCE(cam.total_acima_mediana, 0) / vpc.qtd_registros) AS confiabilidade_leitura_placa_mediana -- confiabilidade baseada na mediana de placa_count
FROM radar_placa_tipo rpt

LEFT JOIN um_erro ue
    ON rpt.placa = ue.placa
    AND rpt.tipoveiculo = ue.tipoveiculo

INNER JOIN veiculos_por_camera vpc
    ON rpt.camera_numero = vpc.camera_numero

LEFT JOIN leituras_frequentes_por_radar lfpr
    ON rpt.camera_numero = lfpr.camera_numero

LEFT JOIN contagem_acima_mediana cam
    ON rpt.camera_numero = cam.camera_numero

WHERE rpt.placa IN (SELECT placa FROM placa_muito_vista)
GROUP BY rpt.camera_numero, vpc.qtd_registros, lfpr.total_leituras_frequentes, cam.total_acima_mediana
