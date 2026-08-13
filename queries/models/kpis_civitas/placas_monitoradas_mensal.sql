{{
    config(
        materialized = 'view'
    )
}}

SELECT
    COUNT(DISTINCT(placa)) AS placas_monitoradas,
    DATE_TRUNC(DATE(data_hora, 'America/Sao_Paulo'), MONTH) AS mes
FROM {{ ref('placas_monitoradas') }}
GROUP BY DATE_TRUNC(DATE(data_hora, 'America/Sao_Paulo'), MONTH)
ORDER BY mes