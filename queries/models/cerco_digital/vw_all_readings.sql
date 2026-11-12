{{
    config(
        materialized='view'
    )
}}


SELECT
  a.*
FROM {{ ref('vw_all_readings_pre_filter') }} a
LEFT JOIN {{ ref('quarentena_lpr_dinamica') }} b
ON DATE(a.datahora, 'America/Sao_Paulo') = b.data_dia 
    AND a.camera_numero = b.codigo_equipamento 
    AND a.id_ponto_coleta = b.id_ponto_coleta
LEFT JOIN {{ ref('placas_blacklist') }} c
ON a.placa = c.placa
WHERE b.codigo_equipamento IS NULL
    AND c.placa IS NULL
    AND
-- remove erroneous future readings
    TIMESTAMP_DIFF(datahora_captura, datahora, SECOND) > -300

      