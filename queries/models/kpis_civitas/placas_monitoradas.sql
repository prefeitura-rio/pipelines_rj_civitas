{{
    config(
        materialized = 'ephemeral'
    )
}}

SELECT
    JSON_VALUE(body, '$.plate') AS placa,
    timestamp AS data_hora
FROM {{ source('brutos_app_civitas', 'userhistory') }}
WHERE path = '/cars/monitored'
    AND status_code >= 200
    AND status_code < 300
    AND method = 'POST'
   