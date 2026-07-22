WITH all_cameras AS (
  SELECT
  record_id,
  mascara as codigo_camera,
  endereco,
  ip_da_camera as host,
  modelo,
  vms_ou_sentry AS sistema,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude,
  IF(status_da_camera = 'Online', 'UP', 'DOWN') AS status,
  camera_furtada = 'Sim' AS camera_furtada,
  DATE(SAFE.TIMESTAMP(data_da_identificacao_do_furto)) AS data_furto,
  COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_de_implantacao_da_camera), DATE(SAFE.TIMESTAMP(data_de_implantacao_da_camera))) AS data_implantacao,
  COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_da_aprovacao_rdo), DATE(SAFE.TIMESTAMP(data_da_aprovacao_rdo))) AS data_aprovacao_rdo,
  TIMESTAMP_TRUNC(SAFE_CAST(updated_at AS TIMESTAMP), SECOND) AS updated_at,
  timestamp_insercao  
FROM {{ source('stg_cerco_digital', 'cameras_civitas') }}
QUALIFY ROW_NUMBER() OVER(PARTITION BY mascara ORDER BY updated_at DESC) = 1
)

SELECT * FROM all_cameras
  WHERE REGEXP_CONTAINS(codigo_camera, r'^[0-9]{7}$')
    AND REGEXP_CONTAINS(host, r'^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
    AND modelo IN ('FIXA', 'PTZ', 'PANORÂMICA', 'LPR')
    AND sistema IN ('VMS', 'SENTRY')
    AND latitude IS NOT NULL
    AND longitude IS NOT NULL
