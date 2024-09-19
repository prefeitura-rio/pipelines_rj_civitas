{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id_denuncia',
        partition_by={
            "field": "data_denuncia",
            "data_type": "datetime",
            "granularity": "month",
        },
        cluster_by = ["timestamp_insercao"]
    )
}}
-- CTE for retrieving id_denuncia from denuncias_api table (newest)
WITH id_new_reports AS (
  SELECT DISTINCT
    id_denuncia
  FROM
    {{ ref('stg_denuncias_api') }}
)

{% if is_incremental() %}
  ,max_timestamp_reports AS (
  SELECT
    MAX(timestamp_insercao) AS max_timestamp
  FROM
    {{ this }}
  )
  SELECT
    *
  FROM
    {{ ref('stg_denuncias_api') }}
  WHERE
    timestamp_insercao > (SELECT max_timestamp FROM max_timestamp_reports)

{% else %}
  -- First execution (full refresh)
  -- Keeping the most recent data when id_denuncia is in the denuncias_api and denuncias_historico tables
  SELECT
    *
  FROM
    {{ ref('stg_denuncias_historico') }}
  WHERE
    id_denuncia NOT IN (SELECT id_denuncia FROM id_new_reports)

  UNION ALL

  SELECT
    *
  FROM
    {{ref('stg_denuncias_api')}}

{% endif %}
