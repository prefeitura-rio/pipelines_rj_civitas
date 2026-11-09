{{
    config(
        materialized='view'
    )
}}


SELECT
  a.*
FROM {{ ref('vw_all_readings_pre_filter') }} a
LEFT JOIN {{ ref('quarentena_lpr_dinamica') }} b
ON DATE(a.datahora, 'America/Sao_Paulo') = b.data_dia AND a.camera_numero = b.codigo_equipamento
WHERE b.codigo_equipamento IS NULL
    AND
-- remove erroneous future readings
    TIMESTAMP_DIFF(datahora_captura, datahora, SECOND) > -300

      