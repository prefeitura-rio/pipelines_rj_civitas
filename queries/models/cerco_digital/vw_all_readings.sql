{{
    config(
        materialized='view'
    )
}}


WITH radar_readings AS (
  SELECT
    datahora_captura,
    placa,
    tipoveiculo,
    velocidade,
    datahora,
    camera_numero,
    camera_latitude,
    camera_longitude,
    empresa,
    image_url
  FROM {{ source('ocr_radar', 'all_readings_raw') }}
  WHERE  -- remove erroneous future readings
    TIMESTAMP_DIFF(datahora_captura, datahora, SECOND) > -300
    AND
      -- camera_numero, camera_latitude and camera_longitude can't be mutually empty/null
      (
        (camera_numero IS NOT NULL AND camera_numero NOT IN ('', '0'))
        OR (camera_latitude != 0.0 AND camera_longitude != 0.0))
    AND NOT REGEXP_CONTAINS(camera_numero, r'[a-zA-Z]')
    -- camera_numero can't contain letters
    -- plate must have 7 digits or be an empty string
    AND (
      placa NOT IN (
        'P0L1C14')  -- TODO: create table to blacklist plates (error in plate detection)
      AND (placa = '' OR LENGTH(placa) = 7))
  QUALIFY
    (
      ROW_NUMBER()
        OVER (
          PARTITION BY datahora, placa, camera_numero, empresa
          ORDER BY datahora_captura DESC
        ))
    = 1
),
normalized_readings AS (
  SELECT
    a.datahora_captura,
    CASE WHEN a.placa = '' THEN '-------' ELSE a.placa END AS placa,
    b.normalized AS tipoveiculo,
    a.velocidade,
    a.datahora,
    a.camera_numero,
    -ABS(a.camera_latitude) camera_latitude,
    -ABS(a.camera_longitude) camera_longitude,
    a.empresa
  FROM radar_readings a
  LEFT JOIN {{ source('ocr_radar', 'tipoveiculo_mapping') }} b
    ON UPPER(TRIM(a.tipoveiculo)) = UPPER(TRIM(b.raw))
),
camera_numero_codcet AS (
  SELECT
    a.datahora_captura,
    a.placa,
    a.tipoveiculo,
    a.velocidade,
    a.datahora,
    COALESCE(b.codcet, a.camera_numero) AS camera_numero,
    a.camera_latitude,
    a.camera_longitude,
    'CETRIO' AS empresa
  FROM normalized_readings a
  LEFT JOIN {{ ref('equipamento_codcet_camera_numero') }} b
    ON a.camera_numero = b.camera_numero
),
lpr_cameras_readings AS (
  SELECT
    datahora_captura,
    placa,
    tipoveiculo,
    velocidade,
    datahora,
    camera_numero,
    camera_latitude,
    camera_longitude,
    'CIVITAS' AS empresa
  FROM {{ source('lpr', 'readings_raw') }} a
  WHERE
    -- remove erroneous future readings
    TIMESTAMP_DIFF(datahora_captura, datahora, SECOND) > -300
  QUALIFY
    (
      ROW_NUMBER()
        OVER (
          PARTITION BY datahora, placa, camera_numero
          ORDER BY datahora_captura DESC
        ))
    = 1
),
all_readings AS (
  SELECT * FROM camera_numero_codcet
  UNION ALL
  SELECT * FROM lpr_cameras_readings
)
SELECT
  a.datahora_captura,
  a.placa,
  a.tipoveiculo,
  a.velocidade,
  a.datahora,
  a.camera_numero,
  c.id_ponto_coleta,
  -- Coordenadas da tabela dimnensão são mais precisas, por isso priorizamos elas
  COALESCE(c.latitude, a.camera_latitude) AS camera_latitude,
  COALESCE(c.longitude, a.camera_longitude) AS camera_longitude,
  c.local AS localidade,
  c.bairro AS bairro,
  c.sentido,
  a.empresa
FROM 
  all_readings a
JOIN {{ ref('equipamento') }} b ON a.camera_numero = b.codigo_equipamento AND a.empresa = b.origem_equipamento
JOIN {{ ref('ponto_coleta_lpr') }} c ON b.codigo_ponto_coleta = c.codigo_ponto_coleta