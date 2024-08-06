{{
    config(
        materialized='table',
        unique_key='id_denuncia_xpto',
    )
}}
-- Query to select distinct values from stg_denuncias
SELECT DISTINCT
    CONCAT(CAST(id_denuncia AS STRING), CAST(id_xpto AS STRING)) AS id_denuncia_xpto,
    id_denuncia,
    id_xpto
FROM
    {{ref("stg_denuncias")}}
WHERE
    ranking = 1
