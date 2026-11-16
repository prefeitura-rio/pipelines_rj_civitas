{{
    config(
        alias = 'ticket_resposta'
    )
}}

{%- set dias = var('painel_resposta_periodo_dias') %}

WITH hoje AS (
    SELECT CURRENT_DATE('America/Sao_Paulo') AS data
),

periodo AS (
    SELECT
        'Atual' AS periodo_tipo,
        DATE_SUB(data, INTERVAL {{ dias - 1 }} DAY) AS periodo_inicio_data,
        data AS periodo_fim_data
    FROM hoje

    UNION ALL

    SELECT
        'Anterior',
        DATE_SUB(data, INTERVAL {{ 2 * dias - 1 }} DAY),
        DATE_SUB(data, INTERVAL {{ dias }} DAY)
    FROM hoje
),

-- Volume: dia de criação no fuso America/Sao_Paulo.
volume AS (
    SELECT
        periodo.periodo_tipo,
        COUNT(ticket.id_ticket) AS ticket_quantidade,
        COUNTIF(ticket.id_ticket IS NOT NULL AND ticket.conclusao_datahora IS NULL) AS ticket_aberto_quantidade,
        COUNTIF(ticket.conclusao_datahora IS NOT NULL) AS ticket_concluido_quantidade
    FROM periodo
    LEFT JOIN {{ ref('stg_app_civitas__ticket') }} AS ticket
        ON DATE(ticket.criacao_datahora, 'America/Sao_Paulo')
            BETWEEN periodo.periodo_inicio_data AND periodo.periodo_fim_data
    GROUP BY periodo.periodo_tipo
),

-- Resolução e SLA: dia de criação em UTC, como no dashboard de SLA da civitas-api.
ticket_utc AS (
    SELECT
        periodo.periodo_tipo,
        ticket.id_ticket,
        ticket.criacao_datahora,
        ticket.conclusao_datahora
    FROM periodo
    INNER JOIN {{ ref('stg_app_civitas__ticket') }} AS ticket
        ON ticket.criacao_datahora >= TIMESTAMP(periodo.periodo_inicio_data)
        AND ticket.criacao_datahora < TIMESTAMP(DATE_ADD(periodo.periodo_fim_data, INTERVAL 1 DAY))
),

resolucao AS (
    SELECT
        periodo_tipo,
        COUNT(*) AS ticket_criado_quantidade,
        COUNTIF(conclusao_datahora IS NOT NULL) AS ticket_resolvido_quantidade,
        AVG(
            IF(
                conclusao_datahora IS NOT NULL,
                TIMESTAMP_DIFF(conclusao_datahora, criacao_datahora, MICROSECOND) / 86400e6,
                NULL
            )
        ) AS resolucao_media_dias
    FROM ticket_utc
    GROUP BY periodo_tipo
),

sla AS (
    SELECT
        ticket_utc.periodo_tipo,
        COUNT(*) AS servico_concluido_quantidade,
        COUNTIF(
            TIMESTAMP_DIFF(servico.conclusao_datahora, servico.criacao_datahora, MICROSECOND) / 86400e6
                <= ticket_sla.sla_dias
        ) AS servico_sla_cumprido_quantidade
    FROM ticket_utc
    INNER JOIN {{ ref('int_ticket__sla') }} AS ticket_sla
        ON ticket_sla.id_ticket = ticket_utc.id_ticket
    INNER JOIN {{ ref('int_ticket_servico__unificado') }} AS servico
        ON servico.id_ticket = ticket_utc.id_ticket
    WHERE servico.concluido_indicador
        AND servico.conclusao_datahora IS NOT NULL
        AND servico.conclusao_datahora >= servico.criacao_datahora
    GROUP BY ticket_utc.periodo_tipo
)

SELECT
    periodo.periodo_tipo,
    periodo.periodo_inicio_data,
    periodo.periodo_fim_data,
    volume.ticket_quantidade,
    volume.ticket_aberto_quantidade,
    volume.ticket_concluido_quantidade,
    SAFE_DIVIDE(resolucao.ticket_resolvido_quantidade, resolucao.ticket_criado_quantidade) * 100
        AS resolucao_taxa,
    resolucao.resolucao_media_dias,
    SAFE_DIVIDE(sla.servico_sla_cumprido_quantidade, sla.servico_concluido_quantidade) * 100
        AS sla_cumprido_percentual
FROM periodo
LEFT JOIN volume USING (periodo_tipo)
LEFT JOIN resolucao USING (periodo_tipo)
LEFT JOIN sla USING (periodo_tipo)
