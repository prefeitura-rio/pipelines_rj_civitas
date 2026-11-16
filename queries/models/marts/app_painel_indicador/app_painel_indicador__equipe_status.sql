{{
    config(
        alias = 'equipe_status'
    )
}}

WITH periodo AS (
    SELECT
        {{ inicio_dia_operacional(8) }} AS periodo_inicio_datahora,
        {{ inicio_dia_operacional(0) }} AS periodo_fim_datahora
),

-- Tickets ativos em algum momento do período. Os finalizados no período contam para a equipe
-- que os finalizou; os demais, para a equipe responsável.
ticket_classificado AS (
    SELECT
        IF(
            ticket.conclusao_datahora < periodo.periodo_fim_datahora,
            ticket.id_equipe_finalizadora,
            ticket.id_equipe
        ) AS id_equipe,
        IFNULL(ticket.conclusao_datahora < periodo.periodo_fim_datahora, FALSE) AS finalizado_indicador
    FROM {{ ref('stg_app_civitas__ticket') }} AS ticket
    CROSS JOIN periodo
    WHERE ticket.criacao_datahora < periodo.periodo_fim_datahora
        AND (
            ticket.conclusao_datahora IS NULL
            OR ticket.conclusao_datahora >= periodo.periodo_inicio_datahora
        )
),

contagem AS (
    SELECT
        id_equipe,
        COUNT(*) AS ticket_entrada_quantidade,
        COUNTIF(finalizado_indicador) AS ticket_finalizado_quantidade,
        COUNTIF(NOT finalizado_indicador) AS ticket_pendente_quantidade
    FROM ticket_classificado
    WHERE id_equipe IS NOT NULL
    GROUP BY id_equipe
)

SELECT
    equipe.id_equipe,
    equipe.nome AS equipe_nome,
    equipe.exibicao_ordem,
    periodo.periodo_inicio_datahora,
    periodo.periodo_fim_datahora,
    IFNULL(contagem.ticket_entrada_quantidade, 0) AS ticket_entrada_quantidade,
    IFNULL(contagem.ticket_finalizado_quantidade, 0) AS ticket_finalizado_quantidade,
    IFNULL(contagem.ticket_pendente_quantidade, 0) AS ticket_pendente_quantidade
FROM {{ ref('int_equipe__operacional') }} AS equipe
CROSS JOIN periodo
LEFT JOIN contagem
    ON contagem.id_equipe = equipe.id_equipe
