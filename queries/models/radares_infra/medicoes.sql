{{
  config(
    materialized = 'incremental',
    partition_by={
        "field": "datahora",
        "data_type": "date",
        "granularity": "day",
    },
    cluster_by = ["datahora", "empresa", "codcet"],
    unique_key = ["datahora", "empresa", "codcet"]
    )
}}
-- (CTE) named to aggregate data from multiple tables.
WITH new_data AS (
  SELECT
    MAX(datahora_captura) AS max_datahora_captura,
    DATE(datahora, 'America/Sao_Paulo') AS datahora,
    empresa,
    codcet,
    COUNT(1) AS medicoes
  FROM {{ source('ocr_radar', 'all_readings') }}
  WHERE
    {% if is_incremental() %}
        -- Keep only new data
        datahora_captura > (SELECT MAX(max_datahora_captura) FROM {{ this }})
    {% else %}
        EXTRACT(YEAR FROM datahora) >= 2024
    {% endif %}
    AND datahora_captura >= datahora
    GROUP BY
      empresa,
      codcet,
      datahora
)

  {% if is_incremental() %}
    , merged_data AS (
        SELECT
            COALESCE(existing.codcet, new_data.codcet) AS codcet,
            COALESCE(existing.empresa, new_data.empresa) AS empresa,
            COALESCE(existing.datahora, new_data.datahora) AS datahora,
            GREATEST(existing.max_datahora_captura, new_data.max_datahora_captura) AS max_datahora_captura,
            COALESCE(existing.medicoes, 0) + COALESCE(new_data.medicoes, 0) AS medicoes
        FROM
            {{ this }} AS existing
        FULL OUTER JOIN
            new_data
        ON existing.datahora = new_data.datahora
            AND existing.empresa = new_data.empresa
            AND existing.codcet = new_data.codcet
    )

    SELECT * FROM merged_data

  {% else %}
    -- First execution: only new data
    SELECT * FROM new_data
  {% endif %}
