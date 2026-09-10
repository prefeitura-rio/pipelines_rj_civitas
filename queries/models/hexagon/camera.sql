{{
    config(
        materialized='table',
        alias='camera'
    )
}}
-- Tabela de câmeras do VMS Hexagon dc3
  SELECT
    entity_id,
    entity_name,
    codigo_camera,
    status_code,
    status_description,
    timestamp_insercao
  FROM {{ source('hexagon_staging', 'camera') }}
  WHERE timestamp_insercao >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)
  QUALIFY ROW_NUMBER() OVER(PARTITION BY entity_id ORDER BY timestamp_insercao DESC) = 1    --Filtrar entity_id com 2 ou mais codigo_camera
          AND ROW_NUMBER() OVER(PARTITION BY codigo_camera ORDER BY timestamp_insercao DESC) = 1 --Filtrar codigo_camera com 2 ou mais entity_id