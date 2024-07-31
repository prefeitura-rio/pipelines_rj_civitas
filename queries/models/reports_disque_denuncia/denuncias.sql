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
        CAST(xpto_id AS INT) AS id_xpto,
        CAST(NULL AS INT) AS id_classe,
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
        relato
    FROM
        `dd-teste.disque_denuncia.denuncias`
)

SELECT DISTINCT
    *
FROM
    denuncias_cte
