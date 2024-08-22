{{
  config(
    materialized = 'table',
    partition_by={
        "field": "datahora",
        "data_type": "date",
        "granularity": "day",
    },
    cluster_by = ["datahora", "empresa", "camera_numero"]
    )
}}

-- (CTE) named to aggregate data from multiple tables.
WITH ocrs AS (
  SELECT DISTINCT
    *
  FROM `rj-cetrio.ocr_radar.readings_2024*`
)
-- Final query
SELECT
  DATE(datahora, 'America/Sao_Paulo') AS datahora,
  empresa,
  camera_numero,
  COUNT(datahora) medicoes
FROM
  ocrs
WHERE
  DATE(datahora, 'America/Sao_Paulo') >= '2024-05-01'
GROUP BY
  datahora,
  empresa,
  camera_numero
