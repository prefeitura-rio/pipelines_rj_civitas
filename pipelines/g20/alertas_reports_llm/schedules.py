# -*- coding: utf-8 -*-
"""
Schedules for the database dump pipeline
"""

from datetime import datetime, timedelta

import pytz
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from prefeitura_rio.pipelines_utils.io import untuple_clocks as untuple

from pipelines.constants import constants

#####################################
#
# G20 AI Reports
#
#####################################

prompt_enriquecimento = """
Você é um analista de segurança especializado no evento do G20. Sua tarefa é avaliar ocorrências e classificar o risco potencial para a vida e integridade física dos participantes.

Baseie sua resposta **exclusivamente no texto apresentado** e **parafraseie as informações ao máximo, evitando copiar trechos literais**. Não utilize informações externas ao contexto fornecido, de forma a evitar referências externas e assegurar uma análise precisa.

Ao realizar a avaliação, classifique o risco independentemente da localização exata do evento ou da ocorrência. Apresente justificativas claras e objetivas para cada classificação de risco, evitando generalizações.
Preencha todos os campos do JSON com precisão e siga cada instrução na sequência indicada.

Para cada ocorrência, siga as instruções abaixo:


##THOUGHT PROCESS##

### Subtask 1:
- **Descrição**: Identificar o tópico principal e tópicos relacionados com base na descrição da ocorrência.
- **Raciocínio**: Definir tópicos é essencial para classificar e organizar o tipo de ameaça ou situação. Tópicos principais e secundários devem refletir a natureza da ocorrência, como "ameaça à segurança", "protestos" ou "problemas de infraestrutura", o que permite uma resposta focada.
- **Critérios de sucesso**: A escolha do tópico principal e dos relacionados é relevante, justificada pela descrição e evidencia claramente o cenário da ocorrência.

### Subtask 2:
- **Descrição**: Classificar o nível de abrangência e justificar a escolha com detalhes.
- **Raciocínio**: O nível de abrangência define o escopo de impacto da ocorrência. Avaliar corretamente essa dimensão permite uma preparação proporcional à potencial ameaça.
- **Critérios de sucesso**: O nível de abrangência é corretamente escolhido e explicado, levando em conta o alcance possível da ocorrência, desde um estabelecimento até um impacto nacional.

### Subtask 3:
- **Descrição**: Definir a estimativa temporal em minutos até o início da ocorrência e justificar a previsão.
- **Raciocínio**: Estimar o tempo de início da ocorrência é fundamental para priorização de resposta. Em casos de incerteza, "0" será utilizado para representar um tempo indefinido.
- **Critérios de sucesso**: A estimativa temporal é logicamente fundamentada nos dados disponíveis e a explicação da previsão é clara e observável.

### Subtask 4:
- **Descrição**: Avaliar o nível de ameaça à vida e integridade física dos participantes, fornecendo uma justificativa detalhada.
- **Raciocínio**: Avaliar o risco é essencial para priorizar a resposta e proteção de vidas. A classificação deve ser baseada no potencial de dano físico e na probabilidade de evento imediato, categorizando a ameaça como "BAIXO" ou "ALTO".
- **Critérios de sucesso**: A justificativa é detalhada e direta, refletindo o nível de ameaça com base em riscos reais à vida e à integridade física, e a classificação final é consistente com os dados fornecidos.


1. **Tópicos**:
    - Identifique o tópico principal e quaisquer tópicos relacionados com base na descrição da ocorrência.
    - Exemplos de tópicos principais incluem: “ameaça à segurança”, “condições climáticas adversas”, “protestos”, “problemas de infraestrutura”, se necessário, adicione tópicos relacionados para complementar a classificação.
    - Justifique a escolha do tópico principal e dos relacionados com base na descrição do evento.

2. **Nível de Abrangência**:
    - Classifique o nível de impacto com uma justificativa detalhada, usando uma das categorias a seguir:
        - **Casa**: Afeta apenas uma residência/estabelecimento
        - **Quadra**: Impacto limitado à área da quadra
        - **Bairro**: Afeta um bairro inteiro
        - **Região**: Impacta múltiplos bairros
        - **Cidade**: Afeta toda a cidade
        - **Estado**: Impacto em nível estadual
        - **País**: Repercussão nacional
    - Descreva o motivo da escolha do nível de abrangência com base no escopo potencial de impacto.

3. **Avaliação Temporal**:
    - Defina o intervalo de tempo em minutos até o possível início da. ocorrencia, explicando a estimativa com base nos dados disponíveis.
    - Use “0” para tempos indefinidos.
    - Explique como chegou à estimativa para o horário previsto, considerando as informações fornecidas.

4. **Nível de Ameaça**:
    - Avalie o risco e escolha entre os níveis abaixo:
     - **BAIXO**: Risco indireto ou muito improvável (não representa risco direto à vida ou integridade, inclui maus-tratos a animais, transgressões ambientais, trabalhistas, etc.)
     - **ALTO**: Ameaça iminente à vida ou integridade física (inclui tiroteios, bloqueio de vias, manifestações, ameaças de bombas ou terrorismo).
    - Justifique a avaliação da ameaça com uma análise objetiva do risco envolvido, considerando o potencial de dano à vida e integridade física dos participantes.


Ocorrencia :

Data do Relatório (Quando a denúncia chegou à prefeitura): ''',
                cast(data_report as string),
                '''

Categoria: ''',
                categoria,
                '''

Tipo/Subtipo: ''',
                to_json_string(tipo_subtipo),
                '''

Organizações: ''',
                to_json_string(orgaos),
                '''

Descrição: ''',
                descricao,
                '''

Retorne apenas os seguintes campos em JSON:
{
    "main_topic": "tópico principal",
    "related_topics": ["array de tópicos relacionados"],

    "scope_level_explanation": "Explicação para o nível de abrangência",
    "scope_level": "nível de abrangência",

    "predicted_time_explanation": "Explicação para os horários previstos",
    "predicted_time_interval": "valor horário previsto",

    "threat_explanation": "Avaliação detalhada da ameaça",
    "threat_level": "valor nível de ameaça"
}

Lembrete: Complete todas as justificativas com base em dados observáveis e use exemplos práticos se possível para reforçar a coerência na análise.


RETORNE APENAS O JSON, SEM EXPLICAÇÕES
"""

query_enriquecimento = """
with
    source_reports as (
        select
            ifnull(id_report, '') as id_report,
            ifnull(id_source, '') as id_source,
            ifnull(id_report_original, '') as id_report_original,
            ifnull(
                datetime(data_report), cast('' as datetime)
            ) as data_report,
            ifnull(
                array(select ifnull(orgao, '') from unnest(orgaos) as orgao), []
            ) as orgaos,
            ifnull(categoria, '') as categoria,
            array(
                select
                    struct(
                        ifnull(item.tipo, '') as tipo,
                        ifnull(
                            array(
                                select ifnull(sub, '') from unnest(item.subtipo) as sub
                            ),
                            []
                        ) as subtipo
                    )
                from unnest(tipo_subtipo) as item
            ) as tipo_subtipo,
            ifnull(descricao, '') as descricao,
            ifnull(logradouro, '') as logradouro,
            ifnull(numero_logradouro, '') as numero_logradouro,
            ifnull(latitude, cast(0 as float64)) as latitude,
            ifnull(longitude, cast(0 as float64)) as longitude
        from `rj-civitas.integracao_reports.reports`  -- tablesample system(10 percent)
        where
            __date_filter_replacer__
    ),

    prompt_table as (
        select *, concat('''__prompt_replacer__''') as prompt_enriquecimento
        from source_reports
    ),

    prompt_id as (
        select
            cast(
                farm_fingerprint(concat(id_report, prompt_enriquecimento)) as string
            ) as id_enriquecimento,
            *
        from prompt_table
    )

__final_select_replacer__
"""


prompt_relacao = """
Você é um analista de segurança especializado no evento do G20. Sua função é definir se existe uma relação entre a ocorrência e o contexto fornecido.

Baseie sua resposta **exclusivamente no texto apresentado** e **parafraseie as informações ao máximo, evitando copiar trechos literais**. Não utilize informações externas ao contexto fornecido, de forma a evitar referências externas e assegurar uma análise precisa.

Forneça uma avaliação clara e direta, justificando objetivamente se existe ou não uma relação entre a ocorrência e o contexto.

##THOUGHT PROCESS##

### Subtask 1:
- **Descrição**: Extrair e identificar as principais informações sobre a ocorrência.
- **Raciocínio**: Compreender as características da ocorrência (ID, descrição, tópico principal e abrangência) é fundamental para estabelecer comparações com o contexto. Isso permite que o analista identifique elementos únicos que podem influenciar a relação com o contexto.
- **Critérios de sucesso**: Extração correta das informações de ID, descrição, tópico principal e abrangência da ocorrência.

### Subtask 2:
- **Descrição**: Analisar e extrair as principais informações sobre o contexto fornecido.
- **Raciocínio**: Assim como na ocorrência, conhecer detalhadamente o contexto permite ao analista encontrar pontos de conexão com a ocorrência. Esses elementos incluem o tipo, descrição, informações adicionais e local do contexto.
- **Critérios de sucesso**: Extração precisa dos campos de tipo, descrição, informações adicionais e local do contexto.

### Subtask 3:
- **Descrição**: Comparar fatores chave entre a ocorrência e o contexto para determinar se existe relação.
- **Raciocínio**: Comparar os elementos centrais de ambos (tópico principal da ocorrência, tipo e local do contexto) é essencial para decidir se existe uma relação observável entre as duas partes. Esse é um passo crítico para justificar e evidenciar o vínculo entre ocorrência e contexto.
- **Critérios de sucesso**: A análise deve identificar ao menos um fator-chave que justifique a relação entre a ocorrência e o contexto.

### Subtask 4:
- **Descrição**: Determinar e justificar a explicação detalhada da relação entre a ocorrência e o contexto.
- **Raciocínio**: Oferecer uma explicação detalhada da relação (ou ausência dela) fornece clareza e transparência ao analista. Para justificar de forma prática, exemplos de situações similares e dados específicos da ocorrência e
do contexto ajudam a solidificar a análise. De um peso maior para correlação entre lugares proximos.
- **Critérios de sucesso**: A explicação deve ser objetiva, coerente e se basear em dados observáveis, incluindo exemplos práticos quando aplicável.

### Subtask 5:
- **Descrição**: Listar fatores-chave que influenciam a relação.
- **Raciocínio**: Identificar e listar os fatores específicos que indicam a relação entre ocorrência e contexto oferece uma visão estruturada dos elementos de semelhança ou dissonância. Esses fatores guiam a análise e apoiam as justificativas.
- **Critérios de sucesso**: A lista deve incluir fatores relevantes, como semelhança temática, geográfica ou contextual.

### Subtask 6:
- **Descrição**: Calcular o nível de confiança da relação entre ocorrência e contexto em uma escala de 0 a 1.
- **Raciocínio**: Atribuir um valor quantitativo de confiança oferece uma métrica objetiva e ajuda a padronizar a avaliação de relações. Esse valor reflete a força da semelhança entre ocorrência e contexto com base nos fatores observados.
- **Critérios de sucesso**: Definir um valor numérico que se alinha com o nível de similaridade, de forma transparente e proporcional ao contexto e à ocorrência.

### Subtask 7:
- **Descrição**: Determinar e validar o valor final de relação como verdadeiro (true) ou falso (false).
- **Raciocínio**: A decisão final de existência de relação é binária e serve como uma conclusão prática para que outros analistas ou sistemas tomem ações subsequentes.
- **Critérios de sucesso**: Valor booleano final (true ou false) baseado em análise fundamentada e coerente com as evidências e critérios estabelecidos.



Ocorrencia:

Descricao: ''',
                descricao_report,
                '''

Topico principal: ''',
                main_topic_report,
                '''

Abrangencia: ''',
                scope_level_report,
                '''

Contexto:

Tipo: ''',
                tipo_contexto,
                '''

Descricao: ''',
                descricao_contexto,
                '''

Informacoes adicionais: ''',
                informacoes_adicionais_contexto,
                '''
Local: ''',
                local_contexto,
                '''


Retorne apenas os seguintes campos em JSON:
{
  'relation_explanation':'explicacao detalhada do motivo da relacao entre a ocorrencia e o contexto',
  'relation_key_factors' ['lista de fatores que indica a relação entre o contexto e a ocorrencia'],
  'relation_confidence': 'nivel de semelhança entre o contexto e a ocorrencia. Valor entre 0 e 1',
  'relation':'valor da relacao. true/false'
}

Lembrete: Complete todas as justificativas com base em dados observáveis e use exemplos práticos se possível para reforçar a coerência na análise.


RETORNE APENAS O JSON, SEM EXPLICAÇÕES
"""


query_relacao = """
with
    source_data as (
        select
            *
        from `rj-civitas.integracao_reports.reports_enriquecidos`
        where __date_filter_replacer__

    ),

    filtered_occurrences as (
        select
            a.id_report,
            a.id_source,
            a.id_enriquecimento,
            a.id_report_original,
            a.data_report,
            a.orgaos as orgaos_report,
            a.categoria as categoria_report,
            a.tipo_subtipo as tipo_subtipo_report,
            a.descricao as descricao_report,
            a.latitude as latitude_report,
            a.longitude as longitude_report,
            a.main_topic as main_topic_report,
            a.scope_level as scope_level_report,
            a.scope_level_explanation as scope_level_explanation_report,
            a.threat_level as threat_level_report,
            a.threat_explanation as threat_explanation_report,
            a.predicted_time_interval as predicted_time_interval_report,
            datetime_add(
                a.data_report, interval safe_cast(a.predicted_time_interval as int64) minute
            ) as predicted_end_time_report,
            a.predicted_time_explanation as predicted_time_explanation_report,
            ifnull(
                cast(a.date_execution as datetime), cast('' as datetime)
            ) as date_execution,
            ifnull(b.id, '') as id_contexto,
            ifnull(b.tipo, '') as tipo_contexto,
            ifnull(b.datahora_inicio, '') as datahora_inicio_contexto,
            ifnull(b.datahora_fim, '') as datahora_fim_contexto,
            ifnull(b.nome, '') as nome_contexto,
            ifnull(b.descricao, '') as descricao_contexto,
            ifnull(b.informacoes_adicionais, '') as informacoes_adicionais_contexto,
            ifnull(b.endereco, '') as endereco_contexto,
            ifnull(b.local, '') as local_contexto,
            ifnull(b.geometria, '') as geometria_contexto,
            ifnull(b.raio_de_busca, cast(5000 as int64)) as raio_de_busca_contexto,
            ifnull(
                cast(a.data_report as datetime), cast('' as datetime)
            ) as data_report_tz,
            ifnull(
                parse_datetime('%d/%m/%Y %H:%M:%S', b.datahora_inicio),
                cast('' as datetime)
            ) as data_inicio_tz
        from source_data a
        cross join (select * from `rj-civitas-dev.g20.contextos`) b
        where
            cast(a.data_report as datetime)
            >= parse_datetime('%d/%m/%Y %H:%M:%S', b.datahora_inicio)
            and cast(a.data_report as datetime)
            <= parse_datetime('%d/%m/%Y %H:%M:%S', b.datahora_fim)
            and lower(a.threat_level) = 'alto'
            and if(
                (cast(a.latitude AS float64) = 0.0 or a.latitude is null)
                or (cast(a.longitude AS float64) = 0.0 or a.longitude is null)
                or (b.geometria is null),
                true,
                st_intersects(
                    st_buffer(
                        st_geogfromtext(b.geometria), coalesce(b.raio_de_busca, 5000)  -- RAIO PADRAO DE 5km
                    ),
                    st_geogpoint(cast(a.longitude AS float64), cast(a.latitude AS float64))
                )
            )
    ),

    prompt_table as (
        select *, concat('''__prompt_replacer__''') as prompt_relacao
        from filtered_occurrences
    ),

    prompt_id as (
        select
            cast(
                farm_fingerprint(concat(id_report, prompt_relacao)) as string
            ) as id_relacao,
            *
        from prompt_table
    )

    __final_select_replacer__
"""


g20_report_clocks = [
    IntervalClock(
        interval=timedelta(minutes=10),
        start_date=datetime(2024, 1, 1, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "query_enriquecimento": query_enriquecimento,
            "prompt_enriquecimento": prompt_enriquecimento,
            "query_relacao": query_relacao,
            "prompt_relacao": prompt_relacao,
            "batch_size": 10,
        },
    )
]
g20_reports_schedule = Schedule(clocks=untuple(g20_report_clocks))
