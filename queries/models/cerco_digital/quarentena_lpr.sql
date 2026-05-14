{{
    config(
        materialized='table'
    )
}}

SELECT
  TRIM(SAFE_CAST(codigo_equipamento AS STRING)) AS codigo_equipamento,
  TRIM(SAFE_CAST(motivo AS STRING)) AS motivo
FROM {{ source('stg_cerco_digital', 'quarentena_lpr') }}