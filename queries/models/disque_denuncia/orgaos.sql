{{
    config(
        materialized='table',
        unique_key='id_orgao',
    )
}}
-- Query to select distinct values from stg_denuncias
SELECT DISTINCT
    id_orgao,
    nome_orgao
FROM
    {{ref("stg_denuncias")}}
WHERE
    ranking = 1
