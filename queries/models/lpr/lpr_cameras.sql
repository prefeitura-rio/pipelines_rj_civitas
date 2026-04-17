{{
  config(
    materialized = "table",
    alias='cameras'
    )
}}

SELECT
  CAST(lane_code AS STRING) AS id_camera,
  TRIM(SPLIT(SPLIT(capture_place_description, '|')[SAFE_OFFSET(0)], '-')[SAFE_OFFSET(0)]) AS codigo_ponto_coleta,
  TRIM(SPLIT(capture_place_description, '|')[SAFE_OFFSET(0)]) AS nome_ponto_coleta,
  INITCAP(TRIM(SPLIT(capture_place_description, '|')[SAFE_OFFSET(1)]))  AS local_ponto_coleta,
  TRIM(SPLIT(SPLIT(capture_place_description, 'SENTIDO ')[SAFE_OFFSET(1)], '|')[SAFE_OFFSET(0)]) AS sentido,
  latitude,
  longitude,
  CASE
    WHEN UPPER(direction) = 'ENTRANCE' THEN 'ENTRADA'
    WHEN UPPER(direction) = 'EXIT' THEN 'SAÍDA'
    WHEN UPPER(direction) = 'INTERNAL' THEN 'INTERNA'
    ELSE NULL
  END AS direcao,
  status

FROM
  {{ source('lpr_staging', 'lanes') }}
WHERE
  UPPER(capture_place_description) NOT LIKE '%LPR-POC%' --avoid POC cameras on prod
QUALIFY ROW_NUMBER() OVER (PARTITION BY lane_code ORDER BY created_at DESC) = 1