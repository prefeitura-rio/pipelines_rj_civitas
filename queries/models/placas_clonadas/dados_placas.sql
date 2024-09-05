{{
  config(
    materialized = 'table',
    cluster_by = ['ano_mes', 'placa']
  )
}}

WITH primeira_ultima_camera_por_dia AS (
  SELECT
    placa,
    EXTRACT(YEAR FROM datahora) AS ano,        -- Extrai o ano da data
    EXTRACT(MONTH FROM datahora) AS mes,       -- Extrai o mês da data
    ARRAY_AGG(camera_numero ORDER BY datahora ASC LIMIT 1)[OFFSET(0)] AS primeira_camera_dia, -- Primeira câmera do dia
    ARRAY_AGG(camera_numero ORDER BY datahora DESC LIMIT 1)[OFFSET(0)] AS ultima_camera_dia    -- Última câmera do dia
  FROM `rj-civitas.placas_clonadas.dados_radares`
  GROUP BY placa, EXTRACT(YEAR FROM datahora), EXTRACT(MONTH FROM datahora), DATE(datahora)
),

mediana_cameras AS (
  SELECT
    placa,
    CONCAT(CAST(ano AS STRING), '-', LPAD(CAST(mes AS STRING), 2, '0')) AS ano_mes,  -- Cria o campo ano_mes aqui
    APPROX_QUANTILES(primeira_camera_dia, 100)[OFFSET(50)] AS mediana_primeira_camera,  -- Mediana das primeiras câmeras
    APPROX_QUANTILES(ultima_camera_dia, 100)[OFFSET(50)] AS mediana_ultima_camera       -- Mediana das últimas câmeras
  FROM primeira_ultima_camera_por_dia
  GROUP BY placa, ano_mes, ano, mes
)

SELECT 
  pv.placa,
  mc.ano_mes,
  GREATEST(150, MAX(pv.velocidade) + 30) AS velocidade_maxima,
  COUNT(pv.placa) AS vezes_vista_total,
  mc.mediana_primeira_camera,  -- Mediana da primeira câmera ao longo do mês
  mc.mediana_ultima_camera     -- Mediana da última câmera ao longo do mês
FROM `rj-civitas.placas_clonadas.dados_radares` pv
JOIN mediana_cameras mc
  ON pv.placa = mc.placa AND EXTRACT(YEAR FROM pv.datahora) = mc.ano AND EXTRACT(MONTH FROM pv.datahora) = mc.mes
GROUP BY pv.placa, mc.ano_mes, mc.ano, mc.mes, mc.mediana_primeira_camera, mc.mediana_ultima_camera