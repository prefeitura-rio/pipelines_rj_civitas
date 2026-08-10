{{
    config(
        materialized = 'view'
    )
}}


WITH old_tickets AS (
  SELECT
    numero_referencia
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2024') }}

  UNION ALL

  SELECT
    numero_referencia
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2025') }}
  
  UNION ALL

  SELECT
    numero_referencia
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2026') }}
),

all_tickets AS (
  SELECT
    UPPER(TRIM(numero_referencia)) AS numero_referencia
  FROM old_tickets

  UNION ALL

  SELECT
    CAST(internal_number AS STRING) AS numero_referencia
  FROM {{ source('brutos_app_civitas', 'tickets') }}
)

SELECT
  (
    SELECT
        count(distinct(plate))
      FROM {{ source('brutos_app_civitas', 'monitoredplate') }}
      WHERE active = TRUE
  ) AS veiculos_monitorados_agora,
  (
    SELECT
      COUNT(DISTINCT(JSON_VALUE(body, '$.plate'))),
    FROM {{ source('brutos_app_civitas', 'userhistory') }}
    WHERE path = '/cars/monitored'
        AND status_code >= 200
        AND status_code < 300
        AND method = 'POST'
  ) AS veiculos_monitorados_total,
  (
    SELECT
      COUNT(*) + 131150 AS alertas_emitidos
    FROM {{ source('cerco_digital', 'alertas') }}
    WHERE status_alerta = 'ENVIADO'
  ) AS alertas_gerados_total,
  (
    SELECT 
      COUNT(DISTINCT(numero_referencia))
    FROM all_tickets
  ) AS casos_apoiados_total