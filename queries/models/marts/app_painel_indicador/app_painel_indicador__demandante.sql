{{
    config(
        alias = 'demandante'
    )
}}

{%- set inicio = "DATE '" ~ var('painel_ticket_civitas_inicio_data') ~ "'" %}

WITH legado AS (
    -- Planilhas de controle: demandas anteriores aos tickets do sistema CIVITAS.
    SELECT
        UPPER(solicitante_nome) AS demandante_nome,
        COUNT(DISTINCT numero_referencia) AS demanda_quantidade
    FROM {{ ref('stg_cerco_digital__controle_demanda') }}
    WHERE entrada_data < {{ inicio }}
        AND solicitante_nome IS NOT NULL
    GROUP BY demandante_nome
),

civitas AS (
    -- Tickets do sistema CIVITAS: o demandante é a operação do ticket.
    SELECT
        COALESCE(NULLIF(TRIM(operacao.nome), ''), 'Sem operação') AS demandante_nome,
        COUNT(DISTINCT ticket.id_ticket) AS demanda_quantidade
    FROM {{ ref('stg_app_civitas__ticket') }} AS ticket
    LEFT JOIN {{ ref('stg_app_civitas__operacao') }} AS operacao
        ON operacao.id_operacao = ticket.id_operacao
    WHERE DATE(ticket.criacao_datahora, 'America/Sao_Paulo') >= {{ inicio }}
    GROUP BY demandante_nome
)

SELECT
    demandante_nome,
    SUM(demanda_quantidade) AS demanda_quantidade,
    {{ inicio }} AS ticket_civitas_inicio_data
FROM (
    SELECT * FROM legado
    UNION ALL
    SELECT * FROM civitas
)
GROUP BY demandante_nome
