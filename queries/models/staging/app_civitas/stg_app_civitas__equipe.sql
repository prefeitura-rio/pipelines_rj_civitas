{{
    config(
        alias = 'equipe'
    )
}}

WITH equipe AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'teams')) }}
)

SELECT
    id AS id_equipe,
    name AS nome,
    description AS descricao,
    is_active AS ativo_indicador,
    created_at AS criacao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM equipe
