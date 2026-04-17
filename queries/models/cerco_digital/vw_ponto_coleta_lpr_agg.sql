{{
    config(
        materialized='view'
    )
}}

WITH reading_metrics AS (
  SELECT
    id_ponto_coleta,
    MAX(datahora) AS datahora_ultima_leitura,
    COUNT(*) AS total_leituras
  FROM {{ ref('vw_all_readings') }}
  WHERE id_ponto_coleta IS NOT NULL
  GROUP BY id_ponto_coleta
)
SELECT
  ap.id_ponto_coleta,
  ap.origem_equipamento,
  ap.codigo_ponto_coleta,
  ap.local,
  ap.bairro,
  ap.sentido,
  ap.latitude,
  ap.longitude,
  ap.status_ativo,
  rm.datahora_ultima_leitura,
  rm.total_leituras,
  CASE
    WHEN rm.datahora_ultima_leitura IS NULL THEN NULL
    WHEN rm.datahora_ultima_leitura >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR) THEN TRUE
    ELSE FALSE
  END AS ativo_ultimas_24h
FROM {{ ref('ponto_coleta_lpr') }} ap
JOIN reading_metrics rm USING(id_ponto_coleta)
