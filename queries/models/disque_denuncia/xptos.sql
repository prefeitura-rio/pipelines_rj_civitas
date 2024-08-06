{{
    config(
        materialized='table',
        unique_key='id_xpto',
    )
}}
-- Query to select distinct values from stg_denuncias
SELECT DISTINCT
    id_xpto,
    descricao_xpto
FROM
    {{ref("stg_denuncias")}}
WHERE
    ranking = 1
    AND COALESCE(
        CAST(id_xpto AS STRING),
        descricao_xpto
    ) IS NOT NULL
