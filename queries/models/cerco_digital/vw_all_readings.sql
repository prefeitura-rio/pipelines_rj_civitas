{{
    config(
        materialized='view'
    )
}}


SELECT
  a.*
FROM {{ ref('vw_all_readings_pre_filter') }} a
LEFT JOIN {{ ref('quarentena_lpr_dinamica') }} b
ON DATE(a.datahora, 'America/Sao_Paulo') = b.data_dia AND a.id_ponto_coleta = b.id_ponto_coleta
WHERE b.id_ponto_coleta IS NULL