{{
    config(
        materialized='table',
        unique_key='id_denuncia',
        partition_by={
            "field": "data_denuncia",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}
-- Common Table Expression (CTE) to clean and format data from the `denuncias` table
WITH denuncias_cte AS (
    SELECT
        CAST(denuncia_id AS INT) AS id_denuncia,
        denuncia_numero AS numero_denuncia,
        DATETIME(datetime_denuncia) AS data_denuncia,
        DATETIME(datetime_difusao) AS data_difusao,
        -- Clean and format address fields
        TRIM(REGEXP_REPLACE(endereco, r'\s+', ' ')) AS endereco,
        TRIM(REGEXP_REPLACE(bairro, r'\s+', ' ')) AS bairro,
        TRIM(REGEXP_REPLACE(municipio, r'\s+', ' ')) AS municipio,
        TRIM(estado) AS estado,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        relato,
        -- Extract filename from the _FILE_NAME field using regex
        REGEXP_EXTRACT(_FILE_NAME, r'(\d{8}_\d{6}_\d{6})_report_disque_denuncia\.csv$') AS nome_arquivo
    FROM
        `dd-teste.disque_denuncia.denuncias`
),

-- CTE to rank records within each id_denuncia partition
denuncias_ranked AS (
    SELECT
        *,
        -- Assign a row number within each id_denuncia partition, ordered by nome_arquivo (descending) and data_difusao (descending)
        ROW_NUMBER() OVER (PARTITION BY id_denuncia ORDER BY nome_arquivo DESC, data_difusao DESC) AS ranking
    FROM
        denuncias_cte
)

-- Final selection of the top-ranked records (ranking = 1) for each id_denuncia
SELECT
    id_denuncia,
    numero_denuncia,
    data_denuncia,
    data_difusao,
    endereco,
    bairro,
    municipio,
    estado,
    latitude,
    longitude,
    relato
FROM
    denuncias_ranked
WHERE
    ranking = 1
