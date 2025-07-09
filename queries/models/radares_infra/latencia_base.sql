{{
 config(
 materialized = 'incremental',
 unique_key = ['empresa', 'data', 'datahora_captura', 'datahora'],
 partition_by={
"field": "data",
"data_type": "datetime",
"granularity": "day",
 },
 cluster_by = ["data", "empresa"],
 incremental_strategy = 'merge'
 )
}}

-- CTE to process datetime data
WITH datas_fuso AS (
    SELECT DISTINCT
        empresa,
        -- Truncate `datahora` to the nearest hour and convert to DATETIME with timezone adjustment
        DATETIME_TRUNC(DATETIME(datahora, 'America/Sao_Paulo'), HOUR) AS data,
        datahora_captura,
        datahora,
        -- Calculate latency in seconds
        TIMESTAMP_DIFF(datahora_captura, datahora, SECOND) AS latencia_segundos,
        -- store the max_datahora_captura for incremental control
        MAX(datahora_captura) OVER() AS max_datahora_captura
    FROM
        {{ source('ocr_radar', 'all_readings') }}
    WHERE
        {% if is_incremental() %}
        -- keep only new data
        datahora_captura > (SELECT MAX(datahora_captura) FROM {{ this }}) AND
        {% endif %}
        EXTRACT(YEAR FROM datahora) >= 2024
        AND datahora_captura >= datahora
)

-- Final query - base data for percentile calculations
SELECT
    empresa,
    data,
    datahora_captura,
    datahora,
    latencia_segundos,
    max_datahora_captura
FROM
    datas_fuso