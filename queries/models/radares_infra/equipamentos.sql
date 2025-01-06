{{
  config(
    materialized = 'table',
    partition_by = {
        "field": "datahora",
        "data_type": "datetime",
        "granularity": "day"
    },
    cluster_by = ["camera_numero", "empresa", "datahora"]
    )
}}
-- CTE to join equipment information with camera numbers
WITH cameras_cetrio AS (
  SELECT
    t2.camera_numero,
    t1.locequip,
    t1.bairro,
    CAST(t1.latitude AS FLOAT64) AS latitude,
    CAST(t1.longitude AS FLOAT64) AS longitude,
    -- Create geographic point from latitude and longitude
    ST_GEOGPOINT(CAST(t1.longitude AS FLOAT64), CAST(t1.latitude AS FLOAT64)) AS geo_coordinates
  FROM {{ source('ocr_radar', 'equipamento') }} t1
  JOIN {{ source('ocr_radar', 'equipamento_codcet_to_camera_numero') }} t2
    ON t1.codcet = t2.codcet
),
-- Convert datahora to date for partitioning
final_data AS (
  SELECT
    DATETIME(r.datahora, 'America/Sao_Paulo') AS datahora,
    r.camera_numero,
    r.empresa,
    c.locequip,
    c.bairro,
    CASE
      WHEN r.empresa = 'SPLICE' THEN r.camera_latitude
      ELSE c.latitude
    END AS latitude,
    CASE
      WHEN r.empresa = 'SPLICE' THEN r.camera_longitude
      ELSE c.longitude
    END AS longitude,
    CASE
      WHEN r.empresa = 'SPLICE' THEN ST_GEOGPOINT(CAST(r.camera_longitude AS FLOAT64), CAST(r.camera_latitude AS FLOAT64))
      ELSE c.geo_coordinates
    END AS geo_coordinates,
    DATETIME(r.datahora_captura, 'America/Sao_Paulo') AS datahora_captura,
    r.placa,
    r.tipoveiculo,
    r.velocidade
  FROM {{ source('ocr_radar', 'all_readings') }} r
  LEFT JOIN cameras_cetrio c
    ON r.camera_numero = c.camera_numero
  WHERE
    DATETIME(r.datahora, 'America/Sao_Paulo') > '2024-05-30'
    AND r.datahora_captura >= r.datahora
)
-- Final query
SELECT
  *
FROM
  final_data