{{
    config(
        alias = 'ticket_servico_reserva_imagem'
    )
}}

WITH ticket_servico_reserva_imagem AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'ticket_image_reservation_services')) }}
)

SELECT
    id AS id_servico,
    ticket_id AS id_ticket,
    concluido AS concluido_indicador,
    period_start AS periodo_inicio_datahora,
    period_end AS periodo_fim_datahora,
    orientation AS orientacao,
    created_at AS criacao_datahora,
    completed_at AS conclusao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM ticket_servico_reserva_imagem
