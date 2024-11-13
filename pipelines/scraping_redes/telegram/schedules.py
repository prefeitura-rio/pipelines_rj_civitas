# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline.
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

tz = pytz.timezone("America/Sao_Paulo")

query_enriquecimento = r"""WITH enriquecimento AS (
  SELECT
    id,
    text,
    CONCAT(
      '''Dada a seguinte mensagem, faça duas tarefas:

1. **Determinar se a mensagem é de cunho informativo**.
   Responda com "true" se a mensagem for uma informação, notícia ou denúncia (exemplo: alerta, notícia de acontecimento, etc.), ou "false" se a mensagem não for informativa (exemplo: mera descrição, pergunta, conversa sem conteúdo informativo).

2. **Inferir o endereço ou a localidade mencionada na mensagem**.
   Se a mensagem contiver um endereço ou uma localidade, extraia a informação mais específica possível. Se houver uma localidade mais precisa, como o nome de um bairro, rua, ponto de referência, estação ou local, preferencialmente extraia esse nível de detalhe.
   Caso não haja nenhuma localidade mencionada, responda com uma string vazia.

O retorno deve ser em formato JSON, como no exemplo abaixo:

{
  "is_related_news": true ou false,
  "locality": "endereço ou localidade ou string vazia"
}

### Exemplo de Entrada:

Mensagem:
"Nos encontramos no centro de São Paulo, perto da estação da luz."

### Exemplo de Saída:

{
  "is_related_news": false,
  "locality": "estação da luz, são paulo"
}

---

### Exemplos de Entrada e Saídas:

**Exemplo 1:**

Mensagem:
"Tiroteio acontecendo agora na cidade nova."

Saída esperada:
{
  "is_related_news": true,
  "locality": "cidade nova"
}

---

**Exemplo 2:**

Mensagem:
"Você viu o último episódio de Game of Thrones?"

Saída esperada:
{
  "is_related_news": false,
  "locality": ""
}

---

**Exemplo 3:**

Mensagem:
"A reunião será em frente ao Teatro Municipal do Rio de Janeiro, na próxima sexta-feira."

Saída esperada:
{
  "is_related_news": false,
  "locality": "teatro municipal do rio de janeiro"
}

---

### Observações:

- **is_related_news**: A chave `is_related_news` deve ser **true** se a mensagem contém uma notícia, alerta ou informação relevante (como um tiroteio, acidente, evento), e **false** caso contrário (se for apenas uma descrição ou conversa sem informação relevante).

- **locality**: A chave `locality` deve conter o endereço ou localidade mais específica possível mencionada na mensagem. Caso o modelo não consiga inferir nenhuma localidade, o valor dessa chave deve ser uma **string vazia** (`""`).

- **Formato**: O retorno deve ser estritamente **JSON puro**, sem nenhum tipo de marcação de código (ex: ```json).

- Texto a ser analisado:''',
          text
  ) AS prompt_column
      FROM
       `rj-civitas.scraping_redes_staging.telegram_messages` WHERE LENGTH(text) > 0
      LIMIT 200

)
select * from enriquecimento
"""

fogo_cruzado_minutely_parameters = {
    "project_id": "rj-civitas",
    "dataset_id": "scraping_redes",
    "table_id_usuarios": "usuarios_monitorados",
    "table_id_messages": "telegram_messages",
    "table_id_chats": "telegram_chats",
    "table_id_enriquecido": "telegram_enriquecido",
    "write_disposition_chats": "WRITE_APPEND",
    "write_disposition_messages": "WRITE_APPEND",
    "start_date": "2024-11-01 00:00:00",
    "end_date": datetime.now(tz=pytz.utc).strftime("%Y-%m-%d %H:%M:%S"),
    "mode": "staging",
    "query_enriquecimento": query_enriquecimento,
}

telegram_interval_clocks = [
    IntervalClock(
        interval=timedelta(minutes=1),
        start_date=datetime(2024, 9, 7, 0, 0, tzinfo=tz),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults=fogo_cruzado_minutely_parameters,
    ),
]

telegram_update_schedule = Schedule(clocks=untuple(telegram_interval_clocks))
