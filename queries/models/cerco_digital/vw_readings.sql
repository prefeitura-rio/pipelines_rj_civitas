{{
    config(
        materialized='view'
    )
}}

WITH readings AS (
  SELECT
    *
  FROM
    {{ source('ocr_radar', 'all_readings_raw') }}
    WHERE
    -- remove erroneous future readings
    TIMESTAMP_DIFF(datahora_captura, datahora, SECOND) > -300 AND
    -- camera_numero, camera_latitude and camera_longitude can't be mutually empty/null
    (
      (camera_numero IS NOT NULL AND camera_numero NOT IN ('', '0')) OR 
      (camera_latitude != 0.0 AND camera_longitude != 0.0)
    )
    AND NOT REGEXP_CONTAINS(camera_numero, r'[a-zA-Z]') -- camera_numero can't contain letters
    -- plate must have 7 digits or be an empty string
    AND (
      placa NOT IN ('P0L1C14') -- TODO: create table to blacklist plates (error in plate detection)
      AND (
        placa = ''
        OR LENGTH(placa) = 7
      )
    )
  GROUP BY
    datahora_captura,
    placa,
    tipoveiculo,
    velocidade,
    datahora,
    camera_numero,
    camera_latitude,
    camera_longitude,
    empresa
),

normalized_readings AS (
  SELECT
    a.datahora_captura,
    CASE 
      WHEN a.placa = ''
      THEN '-------'
      ELSE a.placa
    END AS placa,
    b.normalized AS tipoveiculo,
    a.velocidade,
    a.datahora,
    a.camera_numero,
    - ABS (a.camera_latitude) camera_latitude,
    - ABS (a.camera_longitude) camera_longitude,
    a.empresa
  FROM
    readings a
  LEFT JOIN {{ source('ocr_radar', 'tipoveiculo_mapping') }} b ON UPPER(TRIM(a.tipoveiculo)) = UPPER(TRIM(b.raw))
),

camera_numero_codcet AS (
  SELECT
    a.datahora_captura,
    a.placa,
    a.tipoveiculo,
    a.velocidade,
    a.datahora,
    a.camera_numero,
    -- fallback codcet rule
    COALESCE(
      b.codcet,
      CASE
        WHEN ARRAY_LENGTH(REGEXP_EXTRACT_ALL( a.camera_numero, r'[0-9]')) BETWEEN 9 AND 10
        THEN LPAD(a.camera_numero, 10, '0')
        ELSE NULL
      END
    ) AS codcet,
    a.camera_latitude,
    a.camera_longitude,
    a.empresa,
  FROM
    normalized_readings a
  LEFT JOIN
    {{ source('ocr_radar', 'equipamento_codcet_to_camera_numero') }} b ON a.camera_numero = b.camera_numero
)

SELECT
  a.datahora_captura,
  a.placa,
  a.tipoveiculo,
  a.velocidade,
  a.datahora,
  a.camera_numero,
  a.codcet,
  a.camera_latitude,
  a.camera_longitude,
  a.empresa
FROM 
  camera_numero_codcet a