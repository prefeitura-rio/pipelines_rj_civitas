{#-
    (modelo de staging, tipo de serviço, categoria, coluna de prazo em stg_app_civitas__ticket_sla_configuracao)
    Tipos e prazos seguem o _SERVICE_SLA_MAP da civitas-api; categorias seguem o painel de indicadores.
-#}
{%- set servicos = [
    ('busca_imagem', 'Busca por Imagem', 'Imagem', 'busca_imagem_sla_dias'),
    ('analise_imagem', 'Análise de Imagem', 'Imagem', 'analise_imagem_sla_dias'),
    ('reserva_imagem', 'Reserva de Imagem', 'Imagem', 'outro_sla_dias'),
    ('busca_placa', 'Busca por Placa', 'Relatório', 'busca_placa_sla_dias'),
    ('busca_radar', 'Busca por Radar', 'Relatório', 'busca_radar_sla_dias'),
    ('placa_correlata', 'Placas Correlatas', 'Relatório', 'placa_correlata_sla_dias'),
    ('placa_conjunta', 'Placas Conjuntas', 'Relatório', 'placa_conjunta_sla_dias'),
    ('cerco_eletronico', 'Cerco Eletrônico', 'Cerco Eletrônico', 'cerco_eletronico_sla_dias'),
    ('outro', 'Outros', 'Outros', 'outro_sla_dias'),
    ('atlas_civitas', 'Atlas Civitas', 'Outros', 'atlas_civitas_sla_dias'),
] -%}

WITH configuracao AS (
    -- Mesma regra da civitas-api: vale a configuração mais antiga.
    SELECT *
    FROM {{ ref('stg_app_civitas__ticket_sla_configuracao') }}
    QUALIFY ROW_NUMBER() OVER (ORDER BY criacao_datahora, id_configuracao) = 1
),

servico AS (
    {%- for modelo, tipo, categoria, prazo in servicos %}
    SELECT
        servico.id_servico,
        servico.id_ticket,
        '{{ tipo }}' AS servico_tipo,
        '{{ categoria }}' AS servico_categoria,
        servico.concluido_indicador,
        servico.criacao_datahora,
        servico.conclusao_datahora,
        configuracao.{{ prazo }} AS sla_dias
    FROM {{ ref('stg_app_civitas__ticket_servico_' ~ modelo) }} AS servico
    CROSS JOIN configuracao
    {%- if not loop.last %}

    UNION ALL
    {%- endif %}
    {%- endfor %}
)

-- Só serviços de tickets existentes, como nas consultas da civitas-api.
SELECT servico.*
FROM servico
INNER JOIN {{ ref('stg_app_civitas__ticket') }} AS ticket
    ON ticket.id_ticket = servico.id_ticket
