{{
    config(
        materialized = 'view'
    )
}}

WITH plate_adding_history AS (
    SELECT
        JSON_VALUE(body, '$.plate') AS plate,
        DATE_TRUNC(DATE(timestamp, 'America/Sao_Paulo'), MONTH) AS created_month
    FROM {{ source('brutos_app_civitas', 'userhistory') }}
    WHERE path = '/cars/monitored'
        AND status_code >= 200
        AND status_code < 300
        AND method = 'POST'
    )
    SELECT
        COUNT(DISTINCT(plate)) AS total_placas,
        created_month AS mes
    FROM plate_adding_history
    GROUP BY mes
    ORDER BY mes