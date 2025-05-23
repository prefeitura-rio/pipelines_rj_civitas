{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id_report_original',
    partition_by={
        "field": "data_report",
        "data_type": "timestamp",
        "granularity": "month",
    },
    cluster_by = ["updated_at"]
    )
}}

WITH messages AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY timestamp_creation DESC) AS rn
  FROM
    {{ source('scraping_redes', 'telegram') }}

  {% if is_incremental() %}
  WHERE
    timestamp_creation > COALESCE(
      (SELECT max(updated_at) FROM {{ this }}),
      TIMESTAMP('1900-01-01 00:00:00')
    )
  {% endif %}
  QUALIFY rn = 1
)
SELECT
  'telegram' AS id_source,
  id AS id_report_original,
  timestamp_message AS data_report,
  [''] AS orgaos,
  'Redes Sociais' AS categoria,
  [STRUCT(
    '' AS tipo,
    [''] AS subtipo
  )] AS tipo_subtipo,
  text AS descricao,
  locality AS logradouro,
  '' AS numero_logradouro,
  IF (LOWER(locality) = 'rio de janeiro', NULL, latitude) AS latitude,
  IF (LOWER(locality) = 'rio de janeiro', NULL, longitude) AS longitude,
  timestamp_creation AS updated_at
FROM
  messages
WHERE
  is_news_related = True
  AND state = 'RJ'
