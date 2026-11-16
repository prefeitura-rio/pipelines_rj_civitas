{{
    config(
        alias = 'equipe_servico_categoria'
    )
}}

WITH periodo AS (
    SELECT
        {{ inicio_dia_operacional(8) }} AS periodo_inicio_datahora,
        {{ inicio_dia_operacional(0) }} AS periodo_fim_datahora
),

servico_categoria AS (
    SELECT DISTINCT
        id_ticket,
        servico_categoria
    FROM {{ ref('int_ticket_servico__unificado') }}
),

ticket_periodo AS (
    -- Tickets ativos em algum momento do período.
    SELECT
        ticket.id_ticket,
        ticket.id_equipe,
        ticket.id_equipe_finalizadora,
        ticket.conclusao_datahora,
        periodo.periodo_inicio_datahora,
        periodo.periodo_fim_datahora
    FROM {{ ref('stg_app_civitas__ticket') }} AS ticket
    CROSS JOIN periodo
    WHERE ticket.criacao_datahora < periodo.periodo_fim_datahora
        AND (
            ticket.conclusao_datahora IS NULL
            OR ticket.conclusao_datahora >= periodo.periodo_inicio_datahora
        )
),

contagem AS (
    -- Em aberto ao fim do período: conta para a equipe responsável.
    SELECT ticket.id_equipe, categoria.servico_categoria, COUNT(DISTINCT ticket.id_ticket) AS ticket_quantidade
    FROM ticket_periodo AS ticket
    INNER JOIN servico_categoria AS categoria
        ON categoria.id_ticket = ticket.id_ticket
    WHERE ticket.conclusao_datahora IS NULL
        OR ticket.conclusao_datahora >= ticket.periodo_fim_datahora
    GROUP BY ticket.id_equipe, categoria.servico_categoria

    UNION ALL

    -- Finalizado no período: conta para a equipe que finalizou.
    SELECT ticket.id_equipe_finalizadora, categoria.servico_categoria, COUNT(DISTINCT ticket.id_ticket)
    FROM ticket_periodo AS ticket
    INNER JOIN servico_categoria AS categoria
        ON categoria.id_ticket = ticket.id_ticket
    WHERE ticket.conclusao_datahora >= ticket.periodo_inicio_datahora
        AND ticket.conclusao_datahora < ticket.periodo_fim_datahora
    GROUP BY ticket.id_equipe_finalizadora, categoria.servico_categoria
),

contagem_equipe AS (
    SELECT
        id_equipe,
        servico_categoria,
        SUM(ticket_quantidade) AS ticket_quantidade
    FROM contagem
    GROUP BY id_equipe, servico_categoria
),

categoria AS (
    SELECT DISTINCT servico_categoria
    FROM {{ ref('int_ticket_servico__unificado') }}
)

-- Grade completa (equipe operacional x categoria), com zero onde não há tickets.
SELECT
    equipe.id_equipe,
    equipe.nome AS equipe_nome,
    equipe.exibicao_ordem,
    periodo.periodo_inicio_datahora,
    periodo.periodo_fim_datahora,
    categoria.servico_categoria,
    IFNULL(contagem_equipe.ticket_quantidade, 0) AS ticket_quantidade
FROM {{ ref('int_equipe__operacional') }} AS equipe
CROSS JOIN categoria
CROSS JOIN periodo
LEFT JOIN contagem_equipe
    ON contagem_equipe.id_equipe = equipe.id_equipe
    AND contagem_equipe.servico_categoria = categoria.servico_categoria
