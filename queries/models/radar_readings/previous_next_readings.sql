  {{
    config(
      materialized = 'table',
      partition_by = {
          "field": "datahora",
          "data_type": "datetime",
          "granularity": "day"
      },
      cluster_by = ["datahora", "empresa", "camera_numero"]
      )
  }}

  WITH leituras_por_camera AS (
    -- Cálculo da próxima leitura futura e última leitura anterior para cada câmera
    SELECT
      camera_numero,
      empresa,
      datahora,
      DATE(datahora) AS dia, -- Extrair apenas a data
      LEAD(datahora) OVER (PARTITION BY camera_numero ORDER BY datahora ASC) AS proxima_leitura_futura, -- Próxima leitura futura
      LAG(datahora) OVER (PARTITION BY camera_numero ORDER BY datahora ASC) AS ultima_leitura_anterior -- Última leitura anterior
    FROM `rj-cetrio.ocr_radar.readings_2024_*`
  ),

  -- Gerar todas as combinações de câmeras e dias
  cameras_e_datas AS (
    SELECT
      DISTINCT lc.camera_numero, -- Mantém todas as câmeras
      td AS dia -- Junta todas as datas
    FROM (SELECT DISTINCT camera_numero FROM leituras_por_camera) lc
    CROSS JOIN UNNEST(GENERATE_DATE_ARRAY('2024-05-30', CURRENT_DATE())) AS td
  ),

  -- Unir as leituras reais com todas as datas
  uniao_leituras AS (
    SELECT
      cd.camera_numero,
      cd.dia,
      lc.datahora,
      lc.proxima_leitura_futura,
      lc.ultima_leitura_anterior,
      lc.empresa
    FROM cameras_e_datas cd
    LEFT JOIN leituras_por_camera lc
      ON cd.camera_numero = lc.camera_numero
      AND cd.dia = DATE(lc.datahora)
  )

  -- Propagar a próxima leitura futura e última leitura anterior corretamente

  SELECT
    camera_numero,
    empresa
    COALESCE(datahora, TIMESTAMP(dia)) AS datahora, -- Substitui datahora nulo pela data como timestamp
    -- Propagação da última leitura válida
    COALESCE(
      ultima_leitura_anterior,
      LAG(datahora) OVER (PARTITION BY camera_numero ORDER BY dia ASC),
      MAX(datahora) OVER (PARTITION BY camera_numero ORDER BY dia ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS ultima_leitura_valida,
    COALESCE(proxima_leitura_futura,
      MAX(proxima_leitura_futura) OVER (PARTITION BY camera_numero ORDER BY dia ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS proxima_leitura_valida
  FROM uniao_leituras
