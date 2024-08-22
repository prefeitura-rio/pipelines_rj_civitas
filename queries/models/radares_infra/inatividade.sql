{{
  config(
    materialized = 'table',
    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by = ["date", "empresa"]
    )
}}

-- CTE to generate a list of dates
WITH dates AS (
  SELECT
    date
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2024-05-30', current_date('America/Sao_Paulo'))) AS date
),
-- CTE to retrieve distinct camera numbers and companies for previously generated dates
camera_data AS (
  SELECT
    d.date,
    r.camera_numero,
    r.empresa
  FROM
    dates d
  JOIN (
    SELECT DISTINCT
      camera_numero,
      empresa
    FROM
      `rj-cetrio.ocr_radar.readings_2024*`
    WHERE
      DATE(datahora, 'America/Sao_Paulo') > '2024-05-30'
  ) r
  ON 1=1
),
-- CTE to calculate the previous capture time for each camera and company
RadarActivity AS (
  SELECT
    camera_numero,
    empresa,
    LAG(TIMESTAMP(DATETIME(datahora, 'America/Sao_Paulo'))) OVER (PARTITION BY camera_numero ORDER BY datahora) AS prev_capture,
    TIMESTAMP(DATETIME(datahora, 'America/Sao_Paulo')) AS datahora
  FROM
    `rj-cetrio.ocr_radar.readings_2024*`
),
-- CTE to calculate inactivity periods in hours between consecutive captures
InactivityPeriods AS (
  SELECT
    camera_numero,
    empresa,
    datahora,
    prev_capture,
    -- Calculate the difference in hours between the current and previous capture
    TIMESTAMP_DIFF(datahora, prev_capture, HOUR) AS inactivity_hours
  FROM
    RadarActivity
),
-- CTE to identify periods of inactivity exceeding certain thresholds
gaps AS (
  SELECT
    camera_numero,
    empresa,
    DATE(datahora) AS date,
    -- Determine if there were inactivity periods exceeding specific hours
    MAX(CASE WHEN inactivity_hours > 1 THEN 1 ELSE 0 END) AS periods_exceeding_1h,
    MAX(CASE WHEN inactivity_hours > 3 THEN 1 ELSE 0 END) AS periods_exceeding_3h,
    MAX(CASE WHEN inactivity_hours > 6 THEN 1 ELSE 0 END) AS periods_exceeding_6h,
    MAX(CASE WHEN inactivity_hours > 12 THEN 1 ELSE 0 END) AS periods_exceeding_12h,
    MAX(CASE WHEN inactivity_hours > 24 THEN 1 ELSE 0 END) AS periods_exceeding_24h
  FROM
    InactivityPeriods
  GROUP BY
    camera_numero, DATE(datahora), empresa
),
-- CTE to calculate average and median latency per day for each camera and company.
latency_stats_per_day AS (
  SELECT
    empresa,
    camera_numero,
    DATE(datahora, 'America/Sao_Paulo') AS date,
    -- Calculate the average latency in seconds
    AVG(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND)) AS avg_latency,
    -- Calculate the median latency using approximate quantiles
    APPROX_QUANTILES(TIMESTAMP_DIFF(datahora_captura, datahora, SECOND), 100)[OFFSET(50)] AS median_latency
  FROM
    `rj-cetrio.ocr_radar.readings_2024*`
  GROUP BY
    empresa, camera_numero, DATE(datahora)
)
-- Final query
SELECT
  c.camera_numero,
  c.date,
  c.empresa,
   -- Use COALESCE to handle NULL values and provide default 0 for inactivity periods
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
ORDER BY
  c.camera_numero, c.date
