{{
    config(
        alias = 'ticket_servico_atlas_civitas'
    )
}}

WITH ticket_servico_atlas_civitas AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'ticket_atlas_civitas_services')) }}
)

SELECT
    id AS id_servico,
    ticket_id AS id_ticket,
    concluido AS concluido_indicador,
    name AS pessoa_nome,
    email AS pessoa_email,
    cpf AS pessoa_cpf,
    registration AS pessoa_matricula,
    created_at AS criacao_datahora,
    completed_at AS conclusao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM ticket_servico_atlas_civitas
