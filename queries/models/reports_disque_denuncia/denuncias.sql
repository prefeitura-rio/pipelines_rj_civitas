{{
    config(
        materialized='table',
        partition_by={
            "field": "data_denuncia",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}


WITH denuncias_cte AS (
    SELECT
        CAST(denuncia_id AS INT) AS id_denuncia,
        denuncia_numero AS numero_denuncia,
        DATETIME(datetime_denuncia) AS data_denuncia,
        DATETIME(datetime_difusao) AS data_difusao,
        TRIM(REGEXP_REPLACE(endereco, r'\s+', ' ')) AS endereco,
        TRIM(REGEXP_REPLACE(bairro, r'\s+', ' ')) AS bairro,
        TRIM(REGEXP_REPLACE(municipio, r'\s+', ' ')) AS municipio,
        TRIM(estado) AS estado,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) AS longitude,
        relato,
        ROW_NUMBER() OVER (PARTITION BY denuncia_id ORDER BY data_difusao DESC) AS ranking
    FROM
        `rj-civitas.disque_denuncia_staging.denuncias`
)
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
    denuncias_cte
WHERE
    ranking = 1
