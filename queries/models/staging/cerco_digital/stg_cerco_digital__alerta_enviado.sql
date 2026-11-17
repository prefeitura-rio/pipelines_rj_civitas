{{
    config(
        alias = 'alerta_enviado'
    )
}}

SELECT
    total_alertas_enviados AS alerta_enviado_quantidade
FROM {{ source('cerco_digital_alerta', 'vw_total_alertas_enviados') }}
