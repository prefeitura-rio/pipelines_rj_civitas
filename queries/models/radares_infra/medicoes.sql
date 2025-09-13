{{
  config(
    materialized = 'table',
    partition_by = {
        "field": "datahora",
        "data_type": "date",
        "granularity": "day",
    },
    cluster_by = ["datahora", "empresa", "codcet"],
    unique_key = ["datahora", "empresa", "codcet"]
    )
}}
WITH base_cte AS (
    SELECT
        -- Convert UTC timestamp to GMT-3 (America/Sao_Paulo) for output
        -- Source data is in UTC, but we need Brazilian timezone for analysis
        DATE(datahora, 'America/Sao_Paulo') AS datahora,
        empresa,
        codcet,
        COUNT(1) AS medicoes
  FROM
      {{ ref('vw_readings') }}
  GROUP BY
      datahora,
      empresa,
      codcet
)

SELECT * FROM base_cte