{% docs regra_placa_monitorada_ativa %}
Uma placa é considerada **monitorada** quando possui pelo menos um vínculo ativo
(`active = true`) com autoridade solicitante (`brutos_app_civitas.placa_monitorada_vinculo`).

É a mesma regra da civitas-api (`app/modules/monitored_plates/active_rule.py`). A expiração é
responsabilidade da API: um cron desativa os vínculos com `valid_until` vencida. Entre o
vencimento e a execução do cron, o vínculo continua ativo e a placa continua contada, como na API.

A coluna `active` da tabela de origem `monitoredplate` é legada, não é mais atualizada pela API e
**não deve ser usada**.
{% enddocs %}

{% docs regra_caso_apoiado %}
Um **caso apoiado** é uma demanda atendida pela CIVITAS, de uma destas origens:

- **Controle de demandas**: planilhas operacionais de 2024 a 2026
  (`cerco_digital_staging.controle_demandas_operacional_*`), identificadas por
  `numero_referencia`. São dados legados, que não recebem mais registros;
- **Ticket**: tickets do sistema CIVITAS (`brutos_app_civitas.ticket`), identificados pelo
  `id` do ticket.

Regras aplicadas:

- cada identificador é contado uma única vez (identificadores repetidos na planilha são
  deduplicados);
- identificadores vazios são descartados;
- consideram-se apenas casos com data de entrada a partir de 2024-06-01 (início das
  operações). Casos sem data de entrada válida são descartados;
- a data de entrada dos tickets é a data de criação no fuso `America/Sao_Paulo`.
{% enddocs %}

{% docs regra_ticket_servico %}
Cada ticket pode ter vários **serviços**, gravados em uma tabela por tipo na civitas-api. Os
serviços são unidos em uma única relação, com:

- **tipo**: o nome do tipo de serviço usado pela civitas-api (ex.: Busca por Placa);
- **categoria**, usada no painel de indicadores:
  - Imagem: busca, análise e reserva de imagem;
  - Relatório: busca por placa, busca por radar, placas correlatas e placas conjuntas;
  - Cerco Eletrônico: cerco eletrônico;
  - Outros: outros serviços e Atlas Civitas;
- **prazo de SLA** (`sla_dias`): o prazo do tipo de serviço em
  `brutos_app_civitas.ticket_sla_configuracao`. Vale a configuração mais antiga, como na
  civitas-api. A reserva de imagem usa o prazo de outros serviços.

Só entram serviços de tickets existentes (não deletados).
{% enddocs %}

{% docs regra_ticket_sla %}
O prazo de SLA de um ticket é o **maior** prazo entre os tipos de serviço que ele tem, a mesma
regra da civitas-api (`ticket_dashboard_sla_service.py`). Um serviço cumpre o SLA quando o tempo
entre a criação e a conclusão do serviço, em dias, é menor ou igual ao prazo do ticket.
{% enddocs %}

{% docs regra_painel_dia_operacional %}
O **dia operacional** da CIVITAS vai das 07:00 às 07:00 (America/Sao_Paulo). Antes das 07:00, o
dia operacional atual é o que começou às 07:00 do dia anterior. Os períodos de "N dias
operacionais completos" terminam no início do dia operacional atual (macro
`inicio_dia_operacional`).
{% enddocs %}

{% docs regra_painel_equipe_operacional %}
As **equipes operacionais** exibidas no painel são as equipes ativas cujo nome, sem diferenciar
maiúsculas, está na variável `painel_equipes_operacionais` do `dbt_project.yml` (hoje FOX, GOLF,
HOTEL e INDIA). A ordem de exibição é a ordem da variável.
{% enddocs %}

{% docs regra_painel_equipe_desempenho %}
A **média** é a soma dos tickets resolvidos pelas equipes operacionais dividida pela quantidade
de equipes operacionais (inclusive as sem resolução), truncada para inteiro. Cada equipe é
classificada como:

- **Acima**: resolveu mais que a média acrescida da tolerância;
- **Abaixo**: resolveu menos que a média reduzida da tolerância;
- **Dentro**: nos demais casos, inclusive quando a média é zero.

A tolerância é a variável `painel_desempenho_tolerancia` (hoje 10%). Um ticket resolvido é um
ticket concluído no período, atribuído à equipe que o finalizou.
{% enddocs %}

{% docs regra_painel_ticket_resposta %}
Os períodos são os **últimos 28 dias até hoje** (Atual) e os 28 dias anteriores (Anterior), com
"hoje" no fuso America/Sao_Paulo. A duração é a variável `painel_resposta_periodo_dias`.

- **Volume** (tickets, abertos e concluídos): tickets criados no período, pelo dia de criação em
  America/Sao_Paulo.
- **Resolução e SLA**: mesmas regras do resumo do dashboard de SLA da civitas-api
  (`ticket_dashboard_sla_service.py`), inclusive o dia de criação em **UTC**, mantido de propósito
  para os números serem iguais aos da API. A correção para America/Sao_Paulo está pendente nos
  dois sistemas (TODO.md da civitas-api).
  - taxa de resolução: tickets concluídos ÷ tickets criados;
  - tempo médio de resolução: média, em dias, entre criação e conclusão dos tickets concluídos;
  - SLA cumprido: serviços concluídos (com conclusão posterior à criação) cujo tempo entre
    criação e conclusão, em dias, é menor ou igual ao prazo de SLA do ticket
    (`regra_ticket_sla`), dividido pelo total desses serviços.

Os percentuais e médias não são arredondados: o arredondamento para exibição é feito pelo
painel, igual ao da civitas-api.
{% enddocs %}

{% docs regra_painel_demandante %}
O **demandante** de uma demanda é:

- antes de `painel_ticket_civitas_inicio_data` (hoje 2026-08-01): o solicitante registrado nas
  planilhas de controle, em caixa alta. Cada número de referência conta uma vez por solicitante;
- a partir dessa data: a operação do ticket do sistema CIVITAS, pela data de criação em
  America/Sao_Paulo. Tickets sem operação (ou com a operação deletada) contam como
  "Sem operação".

Nomes equivalentes (ex.: "16ª DP" e "16a DP") são consolidados pelo painel, com a tabela de
apelidos aprovados do repositório `civitas-painel-kpis` (`requester_mapping.py`).
{% enddocs %}
