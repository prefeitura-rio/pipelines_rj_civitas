{{
    config(
        materialized='table',
        partition_by={
            "field": "bairro",
            "data_type": "string"
        },
        cluster_by = ["codcet"]
    )
}}

WITH radars AS (
  SELECT
    codcet,
    locequip AS local,
    bairro,
    latitude,
    longitude,
    ST_GEOGPOINT(CAST(longitude AS FLOAT64), CAST(latitude AS FLOAT64)) AS position
  from {{ source('ocr_radar', 'equipamento') }}
),
used_radars AS (
    SELECT
        DISTINCT
        codcet,
        camera_latitude,
        camera_longitude
    FROM
      {{ ref('vw_readings') }}
    WHERE
      datahora >= '2025-06-01' AND -- DO NOT CHANGE, DATE WHEN COMPANIES STARTED SENDING CODCET INSTEAD OF CAMERA_NUMERO
      codcet IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY codcet ORDER BY datahora DESC) = 1 -- Avoid codcet duplicates caused by different latitude and longitude
),

selected_radar AS (
  SELECT
        COALESCE(
          t2.codcet,
          t1.codcet
        ) AS codcet,
        t1.local,
        t1.bairro,
        COALESCE(t1.latitude, t2.camera_latitude) AS latitude_radar,
        COALESCE(t1.longitude, t2.camera_longitude) AS longitude_radar,
        COALESCE(
            t1.position,
            ST_GEOGPOINT(CAST(t2.camera_longitude AS FLOAT64), CAST(t2.camera_latitude AS FLOAT64))
        ) AS position,
    FROM radars t1
    FULL OUTER JOIN used_radars t2
        ON CAST(t1.codcet AS INT64) = SAFE_CAST(t2.codcet AS INT64)

),

cameras AS (
  SELECT
    id_camera,
    nome_camera,
    latitude,
    longitude,
    ST_GEOGPOINT(longitude, latitude) AS position,
    streaming_url
  FROM {{ ref('cameras') }}
),

ranked_cameras AS (
  SELECT
    t2.codcet,
    t2.local,
    t2.bairro,
    t2.latitude_radar,
    t2.longitude_radar,
    t1.id_camera,
    t1.nome_camera,
    t1.streaming_url,
    t1.latitude,
    t1.longitude,
    ST_DISTANCE(t1.position, t2.position) AS distance,
    ROW_NUMBER() OVER (PARTITION BY t2.codcet ORDER BY ST_DISTANCE(t1.position, t2.position)) AS rank
  FROM selected_radar t2
  LEFT JOIN  cameras t1
    ON ST_DWithin(t1.position, t2.position, 100000) -- Large enough distance to include all relevant cameras
  QUALIFY rank <= 5
)

  SELECT
    codcet,
    COALESCE(UPPER(local), 'Desconhecido') local,
    COALESCE(UPPER(bairro), 'Desconhecido') bairro,
    latitude_radar,
    longitude_radar,
    id_camera,
    nome_camera,
    streaming_url,
    latitude,
    longitude,
    distance,
    rank
  FROM ranked_cameras
  WHERE nome_camera is not null
