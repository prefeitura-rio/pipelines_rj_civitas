SELECT
    id_ticket,
    MAX(sla_dias) AS sla_dias
FROM {{ ref('int_ticket_servico__unificado') }}
GROUP BY id_ticket
