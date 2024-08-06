{{
    config(
        materialized='table',
        unique_key='id_classe',
    )
}}
-- Query to select distinct values from stg_denuncias
SELECT DISTINCT
    id_classe,
    descricao_classe
FROM
    {{ref("stg_denuncias")}}
WHERE
    ranking = 1
