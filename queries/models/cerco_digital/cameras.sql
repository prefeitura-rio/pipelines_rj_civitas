{{
    config(
        materialized='table'
    )
}}

WITH base_cte AS (
  SELECT
    TRIM(CameraCode) AS id_camera,
    INITCAP(TRIM(CameraName)) AS nome_camera,
    INITCAP(TRIM(CameraZone)) AS zona_camera,
    SAFE_CAST(Latitude AS FLOAT64) AS latitude,
    SAFE_CAST(Longitude AS FLOAT64) AS longitude,
    REPLACE(REPLACE(TRIM(Streamming), 'app', 'dev'), 'outvideo', 'outvideo3') AS streaming_url
  FROM {{ source('stg_cerco_digital', 'cameras') }}
)
SELECT 
  id_camera,
  nome_camera,
  zona_camera,
  CASE 
    WHEN id_camera = '005859' THEN latitude / 10000 ELSE latitude
  END AS latitude,
  CASE 
    WHEN id_camera = '005859' THEN longitude / 10000 ELSE longitude
  END AS longitude,
  streaming_url
FROM 
  base_cte