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
    SAFE_CAST(codigo_camera AS STRING) AS codigo_camera
  FROM {{ source('hexagon_staging', 'camera') }}