{{
    config(
        alias = 'ticket_servico_placa_correlata'
    )
}}

WITH ticket_servico_placa_correlata AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'ticket_correlated_plate_services')) }}
)

SELECT
    id AS id_servico,
    ticket_id AS id_ticket,
    concluido AS concluido_indicador,
    period_start AS periodo_inicio_datahora,
    period_end AS periodo_fim_datahora,
    interest_interval_minutes AS interesse_intervalo_minutos,
    detection_count AS deteccao_quantidade,
    detection AS deteccao_tipo,
    created_at AS criacao_datahora,
    completed_at AS conclusao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM ticket_servico_placa_correlata
