{{
    config(
        materialized = 'view'
    )
}}

WITH old_tickets AS (
  SELECT
    numero_referencia,
    data_saida
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2024') }}

  UNION ALL

  SELECT
    numero_referencia,
    data_saida
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2025') }}
  
  UNION ALL

  SELECT
    numero_referencia,
    data_saida
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2026') }}
),

all_tickets AS (
  SELECT
    UPPER(TRIM(numero_referencia)) AS id,
    SAFE.PARSE_DATE('%d/%m/%Y', TRIM(data_saida)) AS data_saida
  FROM old_tickets

  UNION ALL

  SELECT
    CAST(id AS STRING) AS id,
    DATE(completed_at) AS data_saida
  FROM {{ source('brutos_app_civitas', 'tickets')}}
)

SELECT
  COUNT(DISTINCT(id)) as casos_apoiados,
  DATE_TRUNC(data_saida, MONTH) as mes
  FROM all_tickets
group by mes
order by mes