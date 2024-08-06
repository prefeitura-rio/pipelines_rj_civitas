{{
    config(
        materialized='table',
        unique_key='id_denuncia_orgao',
    )
}}
-- Query to select distinct values from stg_denuncias
SELECT DISTINCT
    CONCAT(CAST(id_denuncia AS STRING), CAST(id_orgao AS STRING)) AS id_denuncia_orgao,
    id_denuncia,
    id_orgao,
    tipo_orgao
FROM
    {{ref("stg_denuncias")}}
WHERE
    ranking = 1
