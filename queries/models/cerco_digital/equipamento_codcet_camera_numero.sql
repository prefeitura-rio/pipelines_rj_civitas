{{
  config(
    materialized = 'table',
    )
}}

SELECT DISTINCT
    codcet,
    camera_numero
FROM
    {{ source('ocr_radar_staging', 'equipamento_codcet_to_camera_numero') }}
WHERE codcet != "-"