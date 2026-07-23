{{
    config(
        materialized='table',
        alias='camera'
    )
}}
-- Tabela de câmeras do VMS Hexagon dc3
  SELECT
    SAFE_CAST(can_ptz AS BOOLEAN) AS can_ptz,
    SAFE_CAST(entity_id AS STRING) AS entity_id,
    SAFE_CAST(entity_name AS STRING) AS entity_name,
    SAFE_CAST(codigo_camera AS STRING) AS codigo_camera,
    timestamp_insercao
  FROM {{ source('hexagon_staging', 'camera') }}
  WHERE timestamp_insercao >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY entity_id ORDER BY timestamp_insercao DESC) = 1    --Filtrar entity_id com 2 ou mais codigo_camera
          AND ROW_NUMBER() OVER(PARTITION BY codigo_camera ORDER BY timestamp_insercao DESC) = 1 --Filtrar codigo_camera com 2 ou mais entity_id