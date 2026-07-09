{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        partition_by={
            "field": "datetime",
            "data_type": "timestamp",
            "granularity": "month",
        },
        cluster_by = ['source']
    )
}}

SELECT
  id,
  'news' AS source,
  concat(
    'Título: ', 
    COALESCE(c_title_search, ''), 
    '\nSubtítulo: ', 
    COALESCE(c_subtitle_search, ''), 
    '\n', text, 
    '\nAutoria: ', 
    COALESCE(ca_authors, '')
    ) AS text,
  is_relevant,
  tags,
  datetime,
  locations,
  main_location,
  main_location_city,
  main_location_neighborhood,
  main_location_street,
  main_location_full_address,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude

FROM {{ source('palver_staging', 'palver_news_messages') }}
WHERE text IS NOT NULL
{% if is_incremental() %}
AND datetime >= (SELECT MAX(datetime) FROM {{ this }} WHERE source = 'news')
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao DESC) = 1

UNION ALL

SELECT
  id,
  'press' AS source,
  concat(
    'Título: ', 
    COALESCE(c_title_search, ''), 
    '\n', text
    ) AS text,
  is_relevant,
  tags,
  c_processed_at AS datetime,
  locations,
  main_location,
  main_location_city,
  main_location_neighborhood,
  main_location_street,
  main_location_full_address,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude

FROM {{ source('palver_staging', 'palver_press_messages') }}
WHERE text IS NOT NULL
{% if is_incremental() %}
AND c_processed_at >= (SELECT MAX(datetime) FROM {{ this }} WHERE source = 'press')
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao DESC) = 1

UNION ALL

SELECT
  id,
  'radio.medias' AS source,
  concat(
    'Rádio: ', 
    COALESCE(c_radio_name, ''), 
    '\nPrograma: ', 
    COALESCE(c_program_title, ''), 
    '\n', transcript
    ) AS text,
  is_relevant,
  tags,
  datetime,
  locations,
  main_location,
  main_location_city,
  main_location_neighborhood,
  main_location_street,
  main_location_full_address,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude

FROM {{ source('palver_staging', 'palver_radio_medias_messages') }}
WHERE transcript IS NOT NULL
{% if is_incremental() %}
AND datetime >= (SELECT MAX(datetime) FROM {{ this }} WHERE source = 'radio.medias')
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao DESC) = 1

UNION ALL

SELECT
  id,
  'television' AS source,
  concat(
    'Canal: ', 
    COALESCE(c_channel_name, ''), 
    "\nPrograma: ", 
    COALESCE(program_title, ''), 
    '\n', transcript) AS text,
  is_relevant,
  tags,
  datetime,
  locations,
  main_location,
  main_location_city,
  main_location_neighborhood,
  main_location_street,
  main_location_full_address,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude

FROM {{ source('palver_staging', 'palver_television_messages') }}
WHERE transcript IS NOT NULL
{% if is_incremental() %}
AND datetime >= (SELECT MAX(datetime) FROM {{ this }} WHERE source = 'television')
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao DESC) = 1

UNION ALL

SELECT
  id,
  'twitter' AS source,
  text,
  is_relevant,
  tags,
  datetime,
  locations,
  main_location,
  main_location_city,
  main_location_neighborhood,
  main_location_street,
  main_location_full_address,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude

FROM {{ source('palver_staging', 'palver_twitter_messages') }}
WHERE text IS NOT NULL
{% if is_incremental() %}
AND datetime >= (SELECT MAX(datetime) FROM {{ this }} WHERE source = 'twitter')
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao DESC) = 1

UNION ALL

SELECT
  id,
  'whatsapp' AS source,
  text,
  is_relevant,
  tags,
  datetime,
  locations,
  main_location,
  main_location_city,
  main_location_neighborhood,
  main_location_street,
  main_location_full_address,
  SAFE_CAST(latitude AS FLOAT64) AS latitude,
  SAFE_CAST(longitude AS FLOAT64) AS longitude

FROM {{ source('palver_staging', 'palver_whatsapp_messages') }}
WHERE text IS NOT NULL
{% if is_incremental() %}
AND datetime >= (SELECT MAX(datetime) FROM {{ this }} WHERE source = 'whatsapp')
{% endif %}
QUALIFY ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao DESC) = 1