{{
  config(
    materialized = 'table',
    partition_by={
        "field": "data",
        "data_type": "datetime",
        "granularity": "day",
    },
    cluster_by = ["data", "empresa"]
    )
}}
-- CTE to process datetime data
WITH datas_fuso AS (
  SELECT DISTINCT
    empresa,
    -- Truncate `datahora` to the nearest hour and convert to DATETIME with timezone adjustment
    DATETIME_TRUNC(DATETIME(datahora, 'America/Sao_Paulo'), HOUR) AS data,
    TIMESTAMP(DATETIME(datahora_captura, 'America/Sao_Paulo')) AS datahora_captura,
    TIMESTAMP(DATETIME(datahora, 'America/Sao_Paulo')) AS datahora
  FROM
    {{ source('ocr_radar', 'all_readings') }}
  WHERE
    EXTRACT(YEAR FROM datahora) >= 2024
    AND datahora_captura >= datahora
)
-- Final query
SELECT
  empresa,
  data,
  PERCENTILE_CONT(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND), 0.5) OVER(PARTITION BY empresa, datahora) AS latency_50th,
  PERCENTILE_CONT(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND), 0.75) OVER(PARTITION BY empresa, datahora) AS latency_75th,
  PERCENTILE_CONT(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND), 0.95) OVER(PARTITION BY empresa, datahora) AS latency_95th,
  PERCENTILE_CONT(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND), 0.99) OVER(PARTITION BY empresa, datahora) AS latency_99th
FROM
  datas_fuso