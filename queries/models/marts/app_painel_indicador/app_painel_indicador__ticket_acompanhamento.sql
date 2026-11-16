{{
    config(
        alias = 'ticket_acompanhamento'
    )
}}

WITH ticket AS (
    SELECT
        criacao_datahora,
        conclusao_datahora
    FROM {{ ref('stg_app_civitas__ticket') }}
    WHERE id_ticket_tipo IS NULL
        OR id_ticket_tipo NOT IN UNNEST({{ var('painel_ticket_tipos_excluidos') }})
),

dia AS (
    -- Os 7 dias operacionais completos e o dia operacional atual (parcial).
    SELECT
        operacional_data,
        TIMESTAMP(DATETIME(operacional_data, TIME '07:00:00'), 'America/Sao_Paulo') AS periodo_inicio_datahora,
        LEAST(
            TIMESTAMP(
                DATETIME(DATE_ADD(operacional_data, INTERVAL 1 DAY), TIME '07:00:00'),
                'America/Sao_Paulo'
            ),
            CURRENT_TIMESTAMP()
        ) AS periodo_fim_datahora
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            DATE_SUB({{ data_operacional_atual() }}, INTERVAL 7 DAY),
            {{ data_operacional_atual() }}
        )
    ) AS operacional_data
),

contagem AS (
    SELECT
        dia.operacional_data,
        COUNTIF(ticket.criacao_datahora >= dia.periodo_inicio_datahora) AS ticket_criado_quantidade,
        -- Estoque em aberto ao fim do período, incluindo tickets de dias anteriores.
        COUNTIF(
            ticket.criacao_datahora IS NOT NULL
            AND (
                ticket.conclusao_datahora IS NULL
                OR ticket.conclusao_datahora >= dia.periodo_fim_datahora
            )
        ) AS ticket_aberto_quantidade,
        COUNTIF(
            ticket.conclusao_datahora >= dia.periodo_inicio_datahora
            AND ticket.conclusao_datahora < dia.periodo_fim_datahora
        ) AS ticket_concluido_quantidade
    FROM dia
    LEFT JOIN ticket
        ON ticket.criacao_datahora < dia.periodo_fim_datahora
    GROUP BY dia.operacional_data
),

media AS (
    -- Estoque médio em aberto ao fim de cada dia operacional completo.
    SELECT ROUND(AVG(ticket_aberto_quantidade)) AS ticket_aberto_media_quantidade
    FROM contagem
    WHERE operacional_data < (SELECT MAX(operacional_data) FROM contagem)
)

SELECT
    contagem.operacional_data,
    contagem.ticket_criado_quantidade,
    contagem.ticket_aberto_quantidade,
    contagem.ticket_concluido_quantidade,
    contagem.ticket_criado_quantidade - contagem.ticket_concluido_quantidade AS fluxo_liquido_quantidade,
    contagem.ticket_aberto_quantidade
        - LAG(contagem.ticket_aberto_quantidade) OVER (ORDER BY contagem.operacional_data)
        AS ticket_aberto_variacao_quantidade,
    ROUND(
        SAFE_DIVIDE(
            contagem.ticket_aberto_quantidade
                - LAG(contagem.ticket_aberto_quantidade) OVER (ORDER BY contagem.operacional_data),
            LAG(contagem.ticket_aberto_quantidade) OVER (ORDER BY contagem.operacional_data)
        ) * 100,
        2
    ) AS ticket_aberto_variacao_percentual,
    media.ticket_aberto_media_quantidade
FROM contagem
CROSS JOIN media
