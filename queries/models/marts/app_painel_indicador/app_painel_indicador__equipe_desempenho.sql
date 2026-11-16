{{
    config(
        alias = 'equipe_desempenho'
    )
}}

{%- set tolerancia = var('painel_desempenho_tolerancia') %}

WITH periodo AS (
    SELECT
        {{ inicio_dia_operacional(28) }} AS periodo_inicio_datahora,
        {{ inicio_dia_operacional(0) }} AS periodo_fim_datahora
),

resolvido AS (
    SELECT
        ticket.id_equipe_finalizadora AS id_equipe,
        COUNT(*) AS ticket_resolvido_quantidade
    FROM {{ ref('stg_app_civitas__ticket') }} AS ticket
    CROSS JOIN periodo
    WHERE ticket.conclusao_datahora >= periodo.periodo_inicio_datahora
        AND ticket.conclusao_datahora < periodo.periodo_fim_datahora
    GROUP BY ticket.id_equipe_finalizadora
),

equipe AS (
    SELECT
        equipe.id_equipe,
        equipe.nome AS equipe_nome,
        equipe.exibicao_ordem,
        IFNULL(resolvido.ticket_resolvido_quantidade, 0) AS ticket_resolvido_quantidade
    FROM {{ ref('int_equipe__operacional') }} AS equipe
    LEFT JOIN resolvido
        ON resolvido.id_equipe = equipe.id_equipe
),

media AS (
    -- Média inteira (truncada) entre as equipes operacionais, inclusive as sem resolução.
    SELECT CAST(FLOOR(SUM(ticket_resolvido_quantidade) / COUNT(*)) AS INT64) AS ticket_resolvido_media_quantidade
    FROM equipe
)

SELECT
    equipe.id_equipe,
    equipe.equipe_nome,
    equipe.exibicao_ordem,
    periodo.periodo_inicio_datahora,
    periodo.periodo_fim_datahora,
    equipe.ticket_resolvido_quantidade,
    media.ticket_resolvido_media_quantidade,
    equipe.ticket_resolvido_quantidade - media.ticket_resolvido_media_quantidade AS desvio_quantidade,
    CASE
        WHEN media.ticket_resolvido_media_quantidade > 0
            AND equipe.ticket_resolvido_quantidade > media.ticket_resolvido_media_quantidade * (1 + {{ tolerancia }})
            THEN 'Acima'
        WHEN media.ticket_resolvido_media_quantidade > 0
            AND equipe.ticket_resolvido_quantidade < media.ticket_resolvido_media_quantidade * (1 - {{ tolerancia }})
            THEN 'Abaixo'
        ELSE 'Dentro'
    END AS desempenho_tipo
FROM equipe
CROSS JOIN periodo
CROSS JOIN media
