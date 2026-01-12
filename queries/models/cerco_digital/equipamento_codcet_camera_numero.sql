{{
  config(
    materialized = 'table',
    )
}}

SELECT DISTINCT
    codcet,
    camera_numero
FROM
    {{ source('stg_cerco_digital', 'equipamento_codcet_to_camera_numero') }}
WHERE codcet != "-"