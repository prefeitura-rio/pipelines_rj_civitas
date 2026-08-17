{{
    config(
        materialized = 'view'
    )
}}

WITH old_tickets AS (
  SELECT
    numero_referencia,
    data_entrada
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2024') }}

  UNION ALL

  SELECT
    numero_referencia,
    data_entrada
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2025') }}
  
  UNION ALL

  SELECT
    numero_referencia,
    data_entrada
  FROM {{ source('stg_cerco_digital', 'controle_demandas_operacional_2026') }}
),

all_tickets AS (
  SELECT
    UPPER(TRIM(numero_referencia)) AS id,
    SAFE.PARSE_DATE('%d/%m/%Y', TRIM(data_entrada)) AS data_entrada
  FROM old_tickets

  UNION ALL

  SELECT
    CAST(id AS STRING) AS id,
    DATE(created_at) AS data_entrada
  FROM {{ source('brutos_app_civitas', 'tickets')}}
  WHERE _ab_cdc_deleted_at IS NULL
)

SELECT
  COUNT(DISTINCT(id)) as casos_apoiados,
  DATE_TRUNC(data_entrada, MONTH) as mes
  FROM all_tickets
  WHERE COALESCE(data_entrada, DATE('1970-01-01')) >= '2024-06-01'
group by mes
order by mes