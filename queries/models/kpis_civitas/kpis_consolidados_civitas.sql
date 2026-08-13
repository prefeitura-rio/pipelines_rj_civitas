{{
    config(
        materialized = 'view'
    )
}}


SELECT
  (
    SELECT
      COUNT(distinct(plate))
    FROM {{ source('brutos_app_civitas', 'monitoredplate') }}
    WHERE active = TRUE
  ) AS placas_monitoradas_agora,
  (
    SELECT
      COUNT(DISTINCT(placa)),
    FROM {{ref('placas_monitoradas')}}
  ) AS placas_monitoradas_total,
  (
    SELECT
      total_alertas_enviados
    FROM {{ source('cerco_digital', 'vw_total_alertas_enviados') }}
  ) AS alertas_gerados_total,
  (
    SELECT 
      SUM(casos_apoiados)
    FROM {{ ref("casos_apoiados_mensal") }}
  ) AS casos_apoiados_total