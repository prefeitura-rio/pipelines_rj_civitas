{{
    config(
        alias = 'resumo'
    )
}}

SELECT
    placa_monitorada_quantidade,
    caso_apoiado_quantidade,
    alerta_enviado_quantidade,
    carga_datahora
FROM {{ ref('int_indicador__resumo') }}
