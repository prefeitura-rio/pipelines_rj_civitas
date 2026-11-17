{{
    config(
        alias = 'ticket_servico_outro'
    )
}}

WITH ticket_servico_outro AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'ticket_other_services')) }}
)

SELECT
    id AS id_servico,
    ticket_id AS id_ticket,
    concluido AS concluido_indicador,
    orientation AS orientacao,
    created_at AS criacao_datahora,
    completed_at AS conclusao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM ticket_servico_outro
