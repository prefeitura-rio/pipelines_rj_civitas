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

-- Calculate percentiles over all data from the incremental table
SELECT
    empresa,
    data,
    PERCENTILE_CONT(latencia_segundos, 0.5) OVER(PARTITION BY empresa, data) AS latency_50th,
    PERCENTILE_CONT(latencia_segundos, 0.75) OVER(PARTITION BY empresa, data) AS latency_75th,
    PERCENTILE_CONT(latencia_segundos, 0.95) OVER(PARTITION BY empresa, data) AS latency_95th,
    PERCENTILE_CONT(latencia_segundos, 0.99) OVER(PARTITION BY empresa, data) AS latency_99th
FROM
    {{ ref('latencia_base') }}  -- Reference to the incremental table