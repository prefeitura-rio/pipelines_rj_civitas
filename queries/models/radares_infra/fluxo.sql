{{
  config(
    materialized = 'incremental',
    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by = ["date", "empresa", "codcet"],
    unique_key = ["date", "empresa", "codcet"]
  )
}}

WITH new_data AS (
    SELECT
        codcet,
        empresa,
        DATE(datahora, 'America/Sao_Paulo') AS date,
        COUNT(1) AS total_leituras,
        COUNTIF(velocidade = 0) AS total_velocidade_zero,
        MAX(datahora_captura) AS max_datahora_captura
    FROM
        {{ source('ocr_radar', 'all_readings') }}
    WHERE
        datahora >= '2024-01-01 03:00:00'
        AND datahora_captura >= datahora

        {% if is_incremental() %}
            -- Keep only new data
            AND datahora_captura > (SELECT MAX(max_datahora_captura) FROM {{ this }})
        {% endif %}

    GROUP BY
        empresa, codcet, date
)

{% if is_incremental() %}
    , merged_data AS (
        -- Combine existing data with new data
        SELECT
            COALESCE(existing.codcet, new_data.codcet) AS codcet,
            COALESCE(existing.empresa, new_data.empresa) AS empresa,
            COALESCE(existing.date, new_data.date) AS date,
            COALESCE(existing.total_leituras, 0) + COALESCE(new_data.total_leituras, 0) AS total_leituras,
            COALESCE(existing.total_velocidade_zero, 0) + COALESCE(new_data.total_velocidade_zero, 0) AS total_velocidade_zero,
            GREATEST(
                COALESCE(existing.max_datahora_captura, TIMESTAMP('1900-01-01 03:00:00')),
                COALESCE(new_data.max_datahora_captura, TIMESTAMP('1900-01-01 03:00:00'))
            ) AS max_datahora_captura
        FROM
            {{ this }} AS existing
        FULL OUTER JOIN
            new_data
        ON existing.date = new_data.date
            AND existing.empresa = new_data.empresa
            AND existing.codcet = new_data.codcet
    )

    SELECT * FROM merged_data
{% else %}
    -- First execution: only new data
    SELECT * FROM new_data
{% endif %}