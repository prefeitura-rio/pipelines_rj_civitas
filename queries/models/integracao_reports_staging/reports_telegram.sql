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
    cluster_by = ["timestamp_creation"]
    )
}}

WITH messages AS (
  SELECT
    *
  FROM
    {{ source('scraping_redes', 'telegram') }}

  {% if is_incremental() %}
  WHERE
    timestamp_creation > (SELECT max(timestamp_creation) FROM {{ this }})
  {% endif %}
)
SELECT
  'telegram' AS id_source,
  id AS id_report_original,
  timestamp_message AS data_report,
  [''] AS orgaos,
  '' AS categoria,
  [STRUCT(
    '' AS tipo,
    [''] AS subtipo
  )] AS tipo_subtipo,
  text AS descricao,
  locality AS logradouro,
  '' AS numero_logradouro,
  latitude,
  longitude,
  DATETIME(timestamp_creation) AS timestamp_creation
FROM
  messages
WHERE
  is_news_related = True