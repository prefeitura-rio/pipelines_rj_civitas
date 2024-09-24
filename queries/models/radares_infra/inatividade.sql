{{
  config(
    materialized = 'table',
    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by = ["camera_numero", "date"]
    )
}}

-- CTE to generate a list of dates
WITH distinct_cameras AS (
  SELECT DISTINCT
    camera_numero,
    empresa,
    DATE(MIN(datahora)) AS min_date,
    DATE(MAX(datahora)) AS max_date
  FROM
    {{ source('ocr_radar', 'readings_2024') }}
  WHERE
    datahora > '2024-05-30'
  GROUP BY
    camera_numero, empresa
),
dates AS (
  SELECT
    camera_numero,
    empresa,
    date
  FROM
    distinct_cameras c,
    UNNEST(GENERATE_DATE_ARRAY('2024-05-30', current_date())) AS date
),
camera_data AS (
  SELECT
    d.camera_numero,
    d.empresa,
    d.date,
    r.datahora
  FROM
    dates d
  LEFT JOIN
    {{ source('ocr_radar', 'readings_2024') }} r
  ON
    d.camera_numero = r.camera_numero
    AND d.empresa = r.empresa
    AND DATE(r.datahora) = d.date
),
RadarActivity AS (
  SELECT
    camera_numero,
    empresa,
    date,
    datahora,
    LEAD(datahora) OVER (PARTITION BY camera_numero ORDER BY date, datahora) AS next_capture,
    TIMESTAMP(date + INTERVAL 1 DAY) AS end_of_day
  FROM
    camera_data
),
InactivityPeriods AS (
  SELECT
    camera_numero,
    empresa,
    date,
    CASE
      WHEN datahora IS NULL THEN 24 -- Dia inteiro sem leituras
      WHEN next_capture IS NULL THEN TIMESTAMP_DIFF(end_of_day, datahora, HOUR) -- Até o final do dia
      ELSE TIMESTAMP_DIFF(next_capture, datahora, HOUR)
    END AS inactivity_hours
  FROM
    RadarActivity
),
AggregatedInactivity AS (
  SELECT
    camera_numero,
    empresa,
    date,
    SUM(inactivity_hours) AS total_inactivity_hours
  FROM
    InactivityPeriods
  GROUP BY
    camera_numero, empresa, date
),
gaps as (
SELECT
  camera_numero,
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
  camera_numero, date
),
latency_stats_per_day AS (
  SELECT
    empresa,
    camera_numero,
    DATE(datahora) AS date,
    AVG(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND)) AS avg_latency,
    APPROX_QUANTILES(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND), 100)[OFFSET(50)] AS median_latency
  FROM
    {{ source('ocr_radar', 'readings_2024') }}
  GROUP BY
    empresa, camera_numero, DATE(datahora)
)
SELECT DISTINCT
  c.camera_numero,
  c.date,
  c.empresa,
  COALESCE(g.periods_exceeding_1h, 0) AS periods_exceeding_1h,
  COALESCE(g.periods_exceeding_3h, 0) AS periods_exceeding_3h,
  COALESCE(g.periods_exceeding_6h, 0) AS periods_exceeding_6h,
  COALESCE(g.periods_exceeding_12h, 0) AS periods_exceeding_12h,
  COALESCE(g.periods_exceeding_24h, 0) AS periods_exceeding_24h,
  COALESCE(l.avg_latency, 0) AS avg_latency,
  COALESCE(l.median_latency, 0) AS median_latency
FROM
  camera_data c
LEFT JOIN
  gaps g
ON
  c.camera_numero = g.camera_numero
  AND c.date = g.date
  AND c.empresa = g.empresa
LEFT JOIN
  latency_stats_per_day l
ON
  c.camera_numero = l.camera_numero
  AND c.date = l.date
  AND c.empresa = l.empresa
