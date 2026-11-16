{{
    config(
        alias = 'caso_apoiado_serie_historica'
    )
}}

SELECT
    DATE_TRUNC(entrada_data, MONTH) AS mes_data,
    COUNT(*) AS caso_apoiado_quantidade
FROM {{ ref('int_caso_apoiado__unificado') }}
GROUP BY mes_data
