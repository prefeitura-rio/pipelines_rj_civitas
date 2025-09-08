{{
 config(
 materialized = 'incremental',
 unique_key = ['codcet', 'date'],
 partition_by = {
"field": "date",
"data_type": "date",
"granularity": "day"
 },
 cluster_by = ["codcet", "date"],
 incremental_strategy = 'merge'
 )
}}

-- Set incremental threshold to avoid processing the same data twice and avoid subqueries in joins
{% if is_incremental() %}
  {% set max_datahora_captura_query %}
    SELECT MAX(max_datahora_captura) FROM {{ this }}
  {% endset %}
  {% set results = run_query(max_datahora_captura_query) %}
  {% if execute %}
    {% set max_datahora_captura = results.columns[0].values()[0] %}
  {% else %}
    {% set max_datahora_captura = '1900-01-01' %}
  {% endif %}
{% endif %}

-- CTE to generate a list of radars by company and the first and last date of data
WITH distinct_cameras AS (
SELECT DISTINCT
 codcet,
 empresa,
DATE(MIN(datahora), 'America/Sao_Paulo') AS min_date,
DATE(MAX(datahora), 'America/Sao_Paulo') AS max_date,
MAX(datahora_captura) AS max_datahora_captura
FROM
{{ ref('vw_readings') }}
WHERE
 datahora > '2024-05-30 03:00:00'
AND datahora_captura >= datahora
{% if is_incremental() %}
 -- keep only new data
 AND datahora_captura > '{{ max_datahora_captura }}'
{% endif %}
GROUP BY
 codcet, empresa
),

-- CTE to generate a list of dates
dates AS (
SELECT
 codcet,
 empresa,
 date,
 max_datahora_captura
FROM
 distinct_cameras c
CROSS JOIN
 UNNEST(GENERATE_DATE_ARRAY(
   {% if is_incremental() %}
     -- start from the new data date
     DATE('{{ max_datahora_captura }}', 'America/Sao_Paulo')
   {% else %}
     '2024-05-30'
   {% endif %},
   CURRENT_DATE('America/Sao_Paulo')
 )) AS date
),

camera_data AS (
SELECT
 d.codcet,
 d.empresa,
 d.date,
 r.datahora,
 d.max_datahora_captura
FROM
 dates d
JOIN
{{ ref('vw_readings') }} r
ON
 d.codcet = r.codcet
AND d.empresa = r.empresa
AND DATE(r.datahora, 'America/Sao_Paulo') = d.date
AND r.datahora_captura >= r.datahora
{% if is_incremental() %}
 -- keep only new data
 AND r.datahora_captura > '{{ max_datahora_captura }}'
{% endif %}
),

RadarActivity AS (
SELECT
 codcet,
 empresa,
 date,
 datahora,
LEAD(datahora) OVER (PARTITION BY codcet ORDER BY date, datahora) AS next_capture,
TIMESTAMP(date + INTERVAL 1 DAY, 'America/Sao_Paulo') AS end_of_day
FROM
 camera_data
),

InactivityPeriods AS (
SELECT
 codcet,
 empresa,
 date,
CASE
WHEN datahora IS NULL THEN 24 -- All day without readings
WHEN next_capture IS NULL THEN TIMESTAMP_DIFF(end_of_day, datahora, HOUR) -- Until the end of the day
ELSE TIMESTAMP_DIFF(next_capture, datahora, HOUR)
END AS inactivity_hours
FROM
 RadarActivity
),

AggregatedInactivity AS (
SELECT
 codcet,
 empresa,
 date,
SUM(inactivity_hours) AS total_inactivity_hours
FROM
 InactivityPeriods
GROUP BY
 codcet, empresa, date
),

gaps as (
SELECT
 codcet,
 empresa,
 date,
 total_inactivity_hours,
CASE WHEN total_inactivity_hours > 1 THEN 1 ELSE 0 END AS periods_exceeding_1h,
CASE WHEN total_inactivity_hours > 3 THEN 1 ELSE 0 END AS periods_exceeding_3h,
CASE WHEN total_inactivity_hours > 6 THEN 1 ELSE 0 END AS periods_exceeding_6h,
CASE WHEN total_inactivity_hours > 12 THEN 1 ELSE 0 END AS periods_exceeding_12h,
CASE WHEN total_inactivity_hours >= 24 THEN 1 ELSE 0 END AS periods_exceeding_24h
FROM
 AggregatedInactivity
ORDER BY
 codcet, date
),

latency_stats_per_day AS (
SELECT
 empresa,
 codcet,
 DATE(datahora, 'America/Sao_Paulo') AS date,
 AVG(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND)) AS avg_latency,
 APPROX_QUANTILES(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND), 100)[OFFSET(50)] AS median_latency
FROM
{{ ref('vw_readings') }}
WHERE
 datahora_captura >= datahora
{% if is_incremental() %}
 -- keep only new data
 AND datahora_captura > '{{ max_datahora_captura }}'
{% endif %}
GROUP BY
 empresa, codcet, date
)

SELECT DISTINCT
 c.codcet,
 c.date,
 c.empresa,
COALESCE(g.periods_exceeding_1h, 0) AS periods_exceeding_1h,
COALESCE(g.periods_exceeding_3h, 0) AS periods_exceeding_3h,
COALESCE(g.periods_exceeding_6h, 0) AS periods_exceeding_6h,
COALESCE(g.periods_exceeding_12h, 0) AS periods_exceeding_12h,
COALESCE(g.periods_exceeding_24h, 0) AS periods_exceeding_24h,
COALESCE(l.avg_latency, 0) AS avg_latency,
COALESCE(l.median_latency, 0) AS median_latency,
-- Add the max_datahora_captura column for incremental control
c.max_datahora_captura
FROM
 camera_data c
LEFT JOIN
 gaps g
ON
 c.codcet = g.codcet
AND c.date = g.date
AND c.empresa = g.empresa
LEFT JOIN
 latency_stats_per_day l
ON
 c.codcet = l.codcet
AND c.date = l.date
AND c.empresa = l.empresa