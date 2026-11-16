{{
    config(
        alias = 'operacao'
    )
}}

WITH operacao AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'operation')) }}
)

SELECT
    id AS id_operacao,
    title AS nome,
    description AS descricao,
    created_at AS criacao_datahora,
    updated_at AS origem_atualizacao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM operacao
