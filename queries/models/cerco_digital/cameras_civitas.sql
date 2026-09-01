{{
    config(
        materialized='table'
    )
}}

SELECT
  record_id,
  mascara as codigo_camera,
  endereco,
  ip_da_camera as host,
  UPPER(modelo) AS modelo,
  UPPER(vms_ou_sentry) AS sistema,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude,
  IF(UPPER(status_da_camera) = 'ONLINE', 'UP', 'DOWN') AS status,
  UPPER(camera_furtada) = 'SIM' AS camera_furtada,
  DATE(SAFE.TIMESTAMP(data_da_identificacao_do_furto)) AS data_furto,
  COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_de_implantacao_da_camera), DATE(SAFE.TIMESTAMP(data_de_implantacao_da_camera))) AS data_implantacao,
  COALESCE(SAFE.PARSE_DATE('%d/%m/%Y', data_da_aprovacao_rdo), DATE(SAFE.TIMESTAMP(data_da_aprovacao_rdo))) AS data_aprovacao_rdo,
  TIMESTAMP_TRUNC(SAFE_CAST(updated_at AS TIMESTAMP), SECOND) AS updated_at,
  timestamp_insercao  
FROM {{ source('stg_cerco_digital', 'cameras_civitas') }}
WHERE timestamp_insercao >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY)
  AND REGEXP_CONTAINS(mascara, r'^[0-9]{7}$')
  AND REGEXP_CONTAINS(ip_da_camera, r'^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$')
  AND UPPER(modelo) IN ('FIXA', 'PTZ', 'PANORÂMICA', 'LPR')
  AND UPPER(vms_ou_sentry) IN ('VMS', 'SENTRY')
  AND SAFE_CAST(latitude AS FLOAT64) IS NOT NULL
  AND SAFE_CAST(longitude AS FLOAT64) IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY mascara ORDER BY updated_at DESC) = 1
