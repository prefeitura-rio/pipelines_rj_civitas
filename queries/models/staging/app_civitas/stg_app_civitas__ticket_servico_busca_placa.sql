{{
    config(
        alias = 'ticket_servico_busca_placa'
    )
}}

WITH ticket_servico_busca_placa AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'ticket_plate_search_services')) }}
)

SELECT
    id AS id_servico,
    ticket_id AS id_ticket,
    concluido AS concluido_indicador,
    period_start AS periodo_inicio_datahora,
    period_end AS periodo_fim_datahora,
    created_at AS criacao_datahora,
    completed_at AS conclusao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM ticket_servico_busca_placa
