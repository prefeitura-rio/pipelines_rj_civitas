{{
    config(
        materialized='table',
        unique_key='id_tipo',
    )
}}
-- Query to select distinct values from stg_denuncias
SELECT DISTINCT
    id_tipo,
    id_classe,
    descricao_tipo
FROM
    {{ref("stg_denuncias")}}
WHERE
    ranking = 1
