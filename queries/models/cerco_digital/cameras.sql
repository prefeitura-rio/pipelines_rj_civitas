{{
    config(
        materialized='table'
    )
}}

WITH tixxi_base_cleaned AS (
  -- Standardizing data from tixxi cameras table
  SELECT
    TRIM(a.CameraCode) AS codigo_camera,
    INITCAP(TRIM(a.CameraName)) AS nome_camera,
    INITCAP(TRIM(a.CameraZone)) AS zona_camera,
    SAFE_CAST(a.Latitude AS FLOAT64) AS latitude,
    SAFE_CAST(a.Longitude AS FLOAT64) AS longitude,
    REPLACE(REPLACE(TRIM(a.Streamming), 'app', 'dev'), 'outvideo', 'outvideo3') AS streaming_url,
    'TIXXI' AS sistema_origem,
    'COR' AS responsavel,
    b.codigo_tixxi IS NOT NULL AS camera_interna
  FROM {{ source('stg_cerco_digital', 'cameras') }} a
  LEFT JOIN {{ source('stg_cerco_digital', 'cameras_tixxi_indoor')}} b
  ON SAFE_CAST(a.CameraCode AS INT64) = SAFE_CAST(b.codigo_tixxi AS INT64)
  WHERE a.CameraCode IS NOT NULL
  -- Deduplicate based on codigo_camera at source level
  QUALIFY ROW_NUMBER() OVER(PARTITION BY a.CameraCode ORDER BY Latitude DESC) = 1
),

civitas_base_cleaned AS (
  SELECT
    codigo_camera,
    endereco AS nome_camera,
    CAST(NULL AS STRING) AS zona_camera,
    latitude,
    longitude,
    'DC3' AS sistema_origem,
    'CIVITAS' AS responsavel
  FROM {{ ref('cameras_civitas') }}
  WHERE modelo != 'LPR'
    AND sistema = 'VMS'
),

civitas_streaming_url AS (
  SELECT
    a.codigo_camera,
    a.nome_camera,
    a.zona_camera,
    a.latitude,
    a.longitude,
    IF (b.entity_id IS NULL, CAST(NULL AS STRING), CONCAT('{{ env_var("HEXAGON_VMS__RTSP_URL_PREFIX") }}', b.entity_id)) AS streaming_url,
    a.sistema_origem,
    a.responsavel,
    FALSE AS camera_interna
  FROM civitas_base_cleaned a
  JOIN {{ ref('camera') }} b USING (codigo_camera)
),

merged_cameras AS (
  -- Combine both cleaned sources
  SELECT * FROM tixxi_base_cleaned
  UNION ALL
  SELECT * FROM civitas_streaming_url
)

SELECT
  codigo_camera,
  nome_camera,
  zona_camera,
  -- Specific hardcoded fix for camera 005859 (missing floating point in source)
  CASE
    WHEN codigo_camera = '005859' AND latitude < -90 THEN latitude / 10000
    ELSE latitude
  END AS latitude,
  CASE
    WHEN codigo_camera = '005859' AND longitude < -180 THEN longitude / 10000
    ELSE longitude
  END AS longitude,
  streaming_url,
  sistema_origem,
  responsavel,
  camera_interna
FROM merged_cameras
WHERE
  codigo_camera IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL