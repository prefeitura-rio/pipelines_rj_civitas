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

SELECT
  'televisão' AS id_source,
  id AS id_report_original,
  datetime AS data_report,
  [''] AS orgaos,
  'Notícias' AS categoria,
  ARRAY_AGG(STRUCT(
    tag AS tipo,
    [''] AS subtipo
      )) AS tipo_subtipo,
  CONCAT(
    'Canal: ', 
    COALESCE(c_channel_name, ''), 
    "\nPrograma: ", 
    COALESCE(program_title, ''), 
    '\n', transcript) AS descricao,
  CASE 
    WHEN COALESCE(main_location_street, '') != '' THEN main_location_street 
    ELSE COALESCE(main_location_neighborhood, '')
      END AS logradouro,
  '' AS numero_logradouro,
  IF(COALESCE(main_location_neighborhood, '') = '' AND COALESCE(main_location_street, '') = '', 
    NULL, SAFE_CAST(latitude AS FLOAT64)) AS latitude,
  IF(COALESCE(main_location_neighborhood, '') = '' AND COALESCE(main_location_street, '') = '', 
    NULL, SAFE_CAST(longitude AS FLOAT64)) AS longitude,
  timestamp_insercao AS updated_at

FROM {{ source('palver_staging', 'palver_television_messages') }}
LEFT JOIN UNNEST(tags) AS tag
WHERE
  is_relevant = TRUE
  AND main_location_city = 'Rio de Janeiro'
  {% if is_incremental() %}
  AND datetime >= (SELECT MAX(data_report) FROM {{ this }} )
  {% endif %}
GROUP BY ALL
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao DESC) = 1
