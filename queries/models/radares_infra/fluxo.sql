{{
  config(
    materialized = 'table',
    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by = ["date", "empresa", "codcet"],
    unique_key = ["date", "empresa", "codcet"]
  )
}}

WITH base_cte AS (
    SELECT
        codcet,
        empresa,
        DATE(datahora, 'America/Sao_Paulo') AS date,
        COUNT(1) AS total_leituras,
        COUNTIF(velocidade = 0) AS total_velocidade_zero,
        MAX(datahora_captura) AS max_datahora_captura
    FROM
        {{ ref('vw_readings') }}
    WHERE
        datahora >= '2024-01-01 03:00:00'

    GROUP BY
        empresa, codcet, date
)

SELECT * FROM base_cte