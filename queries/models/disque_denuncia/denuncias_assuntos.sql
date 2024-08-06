{{
    config(
        materialized='table',
        unique_key='id_denuncia_assunto',
    )
}}
-- Query to select distinct values from stg_denuncias
SELECT DISTINCT
    CONCAT(CAST(id_denuncia AS STRING), CAST(id_orgao AS STRING)) AS id_denuncia_assunto,
    id_denuncia,
    id_classe,
    id_tipo,
    assunto_principal
FROM
    {{ref("stg_denuncias")}}
WHERE
    ranking = 1
