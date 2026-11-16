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
