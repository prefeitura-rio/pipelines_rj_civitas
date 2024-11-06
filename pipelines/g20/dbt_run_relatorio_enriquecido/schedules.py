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
Você é um analista de segurança especializado no evento do G20.
Sua função é avaliar ocorrências e classificar o risco potencial para a vida e integridade física dos participantes.
Avalie o risco independentemente da localização exata do evento ou ocorrência
Forneça justificativas claras e objetivas para cada classificação, e preencha todos os campos do JSON com precisão.
Siga as instruções passo a passo.

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
    "id_report": "ID da ocorrência",

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
                data_report, cast('' as datetime)
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
            -- and id_source = 'DD'
            -- and id_report in (
                -- "DD2666784",  -- Desmatamento
                -- "DD2666783",  -- Coleta de lixo fora do horário
                -- "DD2163864",  -- Poluição do ar
                -- "DD2163891",  -- Invasão de propriedade e construção irregular
                -- "DD2163866",  -- Violência Doméstica
                -- "DD2163899",  -- Violência contra idosos
                -- "DD2163865",  -- Tiroteio entre quadrilhas
                -- "DD2163867",  -- maus tratos contra animais
                -- "DD2163898",  -- Segurança Pública
                -- "DD2163897",  -- Segurança Pública
                -- "DD2163881",  -- Tráfico de drogas e posse ilegal de armas de fogo
                -- "DD2163893",  -- Violações de leis trabalhistas
                -- "DD2163858",  -- Perturbação do sossego público
                -- "DD2163902",  -- Uso ilegal de energia elétrica
                -- "DD2163885",  -- Uso ilegal de energia elétrica
                -- "DD2666751",  -- Crimes contra a pessoa e armas de fogo
                -- "DD2666614",  -- Construção Irregular e Posse Ilegal de Armas
                -- "DD2666640",  -- Extorsão e Ações de Milícias
                -- "DD2666699",  -- Tráfico de Drogas
                -- "DD2666560",  -- Guarda/comércio ilícito de armas de fogo
                -- "DD2666615",  -- Obstrução de vias públicas
                -- "DD2666517",  -- Abuso de autoridade por policiais militares
                -- "DD2666714",  -- Corrupção Policial
                -- "DD2666723",  -- Tráfico de drogas e crimes relacionados
                -- "DD2666724",  -- Tráfico de drogas
                -- "DD2666566",  -- Roubo/Furto
                -- "DD2666657",  -- Crimes contra o patrimônio
                -- "DD2666730",  -- Tiroteio entre quadrilhas
                -- "DD2666672",  -- Crimes Violentos
                -- "DD2666686"  -- maus tratos contra animais
                -- "DD2666581",  -- Crimes contra o meio ambiente
                -- "DD2666769",  -- maus tratos contra animais
                -- "DD2666645"  -- Lixo acumulado
            -- )
    -- LIMIT 10
    ),

    prompt_table as (
        select *, concat('''__prompt_replacer__''') as prompt_enriquecimento
        from source_reports
    ),

    prompt_id as (
        select
            cast(
                farm_fingerprint(concat(id_report, prompt_enriquecimento)) as string
            ) as id,
            *
        from prompt_table
    )

__final_select_replacer__
"""

parameters_tables = [
    {
        "query_enriquecimento": query_enriquecimento,
        "prompt_enriquecimento": prompt_enriquecimento,
        "batch_size": 10,
    },
]

g20_report_clocks = [
    IntervalClock(
        interval=timedelta(minutes=10),
        start_date=datetime(2024, 1, 1, 0, 0, tzinfo=pytz.timezone("America/Sao_Paulo")),
        labels=[
            constants.RJ_CIVITAS_AGENT_LABEL.value,
        ],
        parameter_defaults={
            "query_enriquecimento": parameters["query_enriquecimento"],
            "prompt_enriquecimento": parameters["prompt_enriquecimento"],
            "batch_size": parameters["batch_size"],
        },
    )
    for count, parameters in enumerate(parameters_tables)
]
g20_reports_schedule = Schedule(clocks=untuple(g20_report_clocks))
