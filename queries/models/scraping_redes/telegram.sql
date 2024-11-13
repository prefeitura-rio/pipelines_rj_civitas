{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id',
        partition_by={
            "field": "timestamp_message",
            "data_type": "timestamp",
            "granularity": "month",
        },
        cluster_by = ['timestamp_creation']
    )
}}
-- Define a (CTE) to add row numbers partitioned by 'id' and ordered by 'timestamp_insercao'.
WITH query AS (
  SELECT
    a.*,
    b.chat_id,
    c.name AS chat_name,
    c.username AS chat_username,
    b.sender_id,
    b.datetime AS timestamp_message
  FROM
    {{ source('stg_scraping_redes', 'telegram_georreferenciado') }} a
  LEFT JOIN
    {{ source('stg_scraping_redes', 'telegram_messages') }} b
  ON
    a.id = b.id
  LEFT JOIN
    {{ source('stg_scraping_redes', 'telegram_chats') }} c
  ON
    b.chat_id = c.id
)
SELECT
  id,
  chat_id,
  sender_id,
  chat_name,
  chat_username,
  timestamp_message,
  text,
  locality,
  latitude,
  longitude,
  formatted_address,
  is_news_related,
  timestamp_creation
FROM
  query
{% if is_incremental() %}
  WHERE timestamp_creation > (SELECT MAX(timestamp_creation) FROM {{ this }})
{% endif %}
