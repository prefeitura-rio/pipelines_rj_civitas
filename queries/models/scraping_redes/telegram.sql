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
WITH query AS (
  SELECT
    a.*,
    b.chat_id,
    c.name AS chat_name,
    c.username AS chat_username,
    b.sender_id,
    b.datetime AS timestamp_message,
    ROW_NUMBER() OVER (PARTITION BY a.id ORDER BY b.timestamp_creation DESC) AS row_num
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
  QUALIFY row_num = 1
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
  state,
  is_news_related,
  timestamp_creation
FROM
  query
WHERE
  UPPER(state) = 'RJ'
{% if is_incremental() %}
AND timestamp_creation > (SELECT MAX(timestamp_creation) FROM {{ this }})
{% endif %}
