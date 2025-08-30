{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    partition_by={
        "field": "datahora",
        "data_type": "date",
        "granularity": "day",
    },
    cluster_by = ["datahora", "empresa", "codcet"],
    unique_key = ["datahora", "empresa", "codcet"]
    )
}}
-- CTE: identifies all partitions containing new data
-- Reason: We need to process the entire partition when retroactive data arrives,
--         otherwise we will lose data.
--         This is necessary because the data is stored in a partitioned table,
--         and we need to process the entire partition when new data arrives.
--         This is a workaround to avoid the need to create a new table with the new data.
WITH
{% if is_incremental() %}
new_partition_dates AS (
    SELECT DISTINCT
        DATE(datahora) AS days
    FROM
       {{ ref('vw_readings') }}
    WHERE
        datahora_captura > (SELECT MAX(max_datahora_captura) FROM {{ this }})
),
{% endif %}
-- (CTE) named to aggregate readings by day, company and radar.
 new_data AS (
    SELECT
        MAX(datahora_captura) AS max_datahora_captura,
        DATE(datahora, 'America/Sao_Paulo') AS datahora,
        empresa,
        codcet,
        COUNT(1) AS medicoes
  FROM
      {{ ref('vw_readings') }}
  WHERE
      {% if is_incremental() %}
          -- ensure that partitioning will be used
          datahora >= (SELECT TIMESTAMP(MIN(days)) FROM new_partition_dates) AND
          -- keep only the partitions containing new data
          DATE(datahora) IN (SELECT days FROM new_partition_dates)
      {% else %}
          EXTRACT(YEAR FROM datahora) >= 2024
      {% endif %}
      GROUP BY
          datahora,
          empresa,
          codcet
)

SELECT * FROM new_data