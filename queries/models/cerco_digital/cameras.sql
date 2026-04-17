{{
    config(
        materialized='table'
    )
}}

WITH tixxi_base_cleaned AS (
  -- Standardizing data from tixxi cameras table
  SELECT
    TRIM(CameraCode) AS codigo_camera,
    INITCAP(TRIM(CameraName)) AS nome_camera,
    INITCAP(TRIM(CameraZone)) AS zona_camera,
    SAFE_CAST(Latitude AS FLOAT64) AS latitude,
    SAFE_CAST(Longitude AS FLOAT64) AS longitude,
    REPLACE(REPLACE(TRIM(Streamming), 'app', 'dev'), 'outvideo', 'outvideo3') AS streaming_url,
    'TIXXI' AS sistema_origem,
    'COR' AS responsavel
  FROM {{ source('stg_cerco_digital', 'cameras') }}
  WHERE CameraCode IS NOT NULL
  -- Deduplicate based on codigo_camera at source level
  QUALIFY ROW_NUMBER() OVER(PARTITION BY CameraCode ORDER BY Latitude DESC) = 1
),

civitas_cameras_vms AS (
  SELECT
    host,
    sistema
  FROM
    {{ source('stg_cerco_digital', 'hosts_cameras_civitas') }}
  WHERE
    sistema = 'VMS'
),

civitas_base_cleaned AS (
  -- Extracting and standardizing data from the civitas staging table
  -- Pattern: "CODE - Name"
  SELECT 
    TRIM(REGEXP_EXTRACT(a.endereco, r'^(.+?)\s*-')) AS codigo_camera,
    INITCAP(TRIM(REGEXP_EXTRACT(a.endereco, r'-\s*(.+)$'))) AS nome_camera,
    CAST(NULL AS STRING) AS zona_camera,
    - ABS(SAFE_CAST(a.latitude AS FLOAT64)) AS latitude,
    - ABS(SAFE_CAST(a.longitude AS FLOAT64)) AS longitude,
    CAST(NULL AS STRING) AS streaming_url,
    'DC3' AS sistema_origem,
    'CIVITAS' AS responsavel
  FROM {{ source('stg_cerco_digital', 'cameras_civitas') }} a
  JOIN civitas_cameras_vms b USING (host)
  -- Only include records that follow the "Code - Name" pattern
  WHERE 
  REGEXP_CONTAINS(a.endereco, r'-') AND
  a.modelo != 'LPR' -- Only videomonitoring cameras are included
  -- Deduplicate based on extracted codigo_camera
  QUALIFY ROW_NUMBER() OVER(PARTITION BY TRIM(REGEXP_EXTRACT(a.endereco, r'^(.+?)\s*-')) ORDER BY a.latitude DESC) = 1
),

merged_cameras AS (
  -- Combine both cleaned sources
  SELECT * FROM tixxi_base_cleaned
  UNION ALL
  SELECT * FROM civitas_base_cleaned
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
  responsavel
FROM merged_cameras
WHERE 
  codigo_camera IS NOT NULL 
  AND latitude IS NOT NULL 
  AND longitude IS NOT NULL