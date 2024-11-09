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
Você é um analista de segurança especializado em avaliar riscos durante o G20.
Sua tarefa é analisar ocorrências e classificar seu risco potencial para participantes do evento.

REGRAS GERAIS:
- Base sua análise APENAS no texto fornecido
- **NÃO faça suposições baseadas em conhecimento externo!**
- Parafraseie ao máximo, evitando citações diretas
- Forneça justificativas objetivas para cada classificação

Processo de Análise:

1. CLASSIFICAÇÃO TEMÁTICA
a. Tópico Principal
    - Identifique o tema central do incidente
    - Escolha UMA categoria que melhor representa:
        * Ameaça à Segurança (crimes, ataques, ameaças)
        * Distúrbio Civil (protestos, manifestações)
        * Infraestrutura (problemas estruturais, serviços)
        * Saúde Pública (epidemias, contaminações)
        * Clima/Ambiente (desastres naturais, poluição)
        * Mobilidade (trânsito, bloqueios)
        * Ordem Pública (desordem, vandalismo)
    - Crie nova categoria se necessário

b. Tópicos Relacionados
    - Liste outros aspectos relevantes do incidente
    - Identifique impactos secundários
    - Considere desdobramentos possíveis

2. AVALIAÇÃO DE IMPACTO
a. Abrangência
    - Avalie a extensão do impacto:
        * Casa: Afeta uma única residência/estabelecimento
        * Quadra: Impacto limitado a algumas edificações
        * Bairro: Afeta área residencial/comercial específica
        * Região: Impacto em múltiplos bairros
        * Cidade: Afeta município inteiro
        * Estado/País: Impacto mais amplo
    - Considere propagação potencial

3. ANÁLISE DE TEMPORALIDADE
- Forneça estimativa em minutos até início da ocorrência (use "0" se indefinido)
- Baseie-se em dados concretos do relato

4. AVALIAÇÃO DE AMEAÇA
- BAIXO:
    * Sem risco direto à vida dos participantes do evento
    * Impacto principalmente logístico/operacional
    * Situação controlável/previsível
    * Baixa probabilidade de escalada

- ALTO:
    * Risco direto à vida ou integridade física
    * Potencial para danos significativos
    * Situação instável/imprevisível
    * Alta probabilidade de escalada

5. TITULAÇÃO
- Máximo 50 caracteres
- Deve refletir os elementos principais da ocorrência


CAMPOS A SEREM ANALISADOS:

1. TÓPICOS
- Principal: Escolha UMA categoria. exemplos: "ameaça à segurança", "clima adverso", "protestos", "infraestrutura", crie novos tópicos se necessário.
- Relacionados: Liste tópicos adicionais relevantes
- Justificativa: Explique brevemente a escolha baseada nos fatos apresentados

2. ABRANGÊNCIA
- Nível: Escolha UM ["Casa", "Quadra", "Bairro", "Região", "Cidade", "Estado", "País"]
- Justificativa: Explique o impacto geográfico potencial

3. TEMPO
- Intervalo:

4. AMEAÇA
- Nível: Classifique como ["BAIXO", "ALTO"]
  * BAIXO: Risco indireto/improvável (ex: infrações ambientais)
  * ALTO: Ameaça direta/iminente (ex: tiroteios, bloqueios)
- Justificativa: Analise o risco à vida/integridade física

5. TÍTULO


Dados da Ocorrência:

Data da Ocorrência (Quando chegou à prefeitura): ''',
                cast(data_report as string),
                '''

Endereço Ocorrência: ''',
                logradouro, ', ', numero_logradouro,
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

    "title_report": "titulo da ocorrencia em no maximo 50 caracteres"
}

Lembrete:
**NÃO faça suposições baseadas em conhecimento externo!**

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
Você é um analista de segurança especializado no G20.
Sua tarefa é determinar se existe alguma relação entre ocorrências reportadas e contextos fornecidos.

REGRAS GERAIS:
- Analise EXCLUSIVAMENTE as informações fornecidas no texto
- NÃO faça suposições baseadas em conhecimento externo
- Parafraseie ao máximo, evitando citações diretas
- Baseie suas conclusões apenas nos fatos apresentados
- **NÃO considere proximidade geográfica ou temporal em suas análises**

Processo de Análise:

1. COMPREENSÃO DA OCORRÊNCIA
- Entenda o evento principal descrito
- Identifique os atores envolvidos
- Compreenda o impacto e alcance do incidente
- Observe características únicas da situação

2. COMPREENSÃO DO CONTEXTO
- Entenda o cenário descrito no contexto
- Identifique o escopo do contexto
- Observe elementos específicos mencionados
- Identifique requisitos ou condições especiais

3. ANÁLISE DE RELAÇÃO
- Procure conexões diretas entre ocorrência e contexto
- Identifique elementos que aparecem em ambos
- Verifique se a ocorrência poderia ser parte do contexto
- Analise se o contexto engloba a situação da ocorrência
- Considere se há contradições entre os relatos
- Avalie se a ocorrência representa um desdobramento do contexto

4. AVALIAÇÃO DE EVIDÊNCIAS
- Liste evidências que suportam a relação
- Identifique possíveis contradições
- Avalie a força das evidências encontradas
- Considere explicações alternativas

5. CONCLUSÃO
- Determine se há relação com base nas evidências
- Atribua um nível de confiança à conclusão
- Justifique a decisão com os elementos mais relevantes

Critérios para Estabelecer Relação:
- Evidências claras conectando os eventos
- Consistência entre as descrições
- Ausência de contradições significativas
- Alinhamento de contexto e circunstâncias
- Complementaridade das informações



Dados da Ocorrência:

Data da Ocorrência (Quando chegou à prefeitura): ''',
                cast(data_report as string),
                '''

Endereço Ocorrência: ''',
                logradouro_report, ', ', numero_logradouro_report,
                '''

Categoria Ocorrência: ''',
                categoria_report,
                '''

Tipo/Subtipo Ocorrência: ''',
                to_json_string(tipo_subtipo_report),
                '''

Organizações Ocorrência: ''',
                to_json_string(orgaos_report),
                '''

Descrição Ocorrência: ''',
                descricao_report,
                '''

Topico principal Ocorrência: ''',
                to_json_string(main_topic_report),
                '''

Topico relacionados Ocorrência: ''',
                to_json_string(related_topics_report),
                '''

Nível ameaça Ocorrência: ''',
                threat_level_report,
                '''

Explicacao nível ameaça Ocorrência: ''',
                threat_explanation_report,
                '''

Abrangencia Ocorrência: ''',
                scope_level_report,
                '''

Explicacao Abrangencia Ocorrência: ''',
                scope_level_explanation_report,
                '''


Dados do Contexto:

Nome Contexto: ''',
                nome_contexto,
                '''

Data Inicio Contexto: ''',
                cast(datahora_inicio_contexto as string),
                '''

Data Fim Contexto: ''',
                cast(datahora_fim_contexto as string),
                '''

Tipo Contexto: ''',
                tipo_contexto,
                '''

Descricao Contexto: ''',
                descricao_contexto,
                '''

Informacoes adicionais Contexto: ''',
                informacoes_adicionais_contexto,
                '''
Local Contexto: ''',
                local_contexto,
                '''

Endereço Contexto: ''',
                endereco_contexto,
                '''

Retorne apenas os seguintes campos em JSON:
{
    "relation_explanation": "Explicação detalhada da análise e conclusão",
    "relation_key_factors": ["Principais evidências que fundamentam a conclusão"],
    "relation_confidence": "Nível de confiança na conclusão (0-1)",
    "relation": "true se houver evidências suficientes de relação, false se não houver",
    "relation_title": "Título descritivo do alerta (máx 50 caracteres)"
}

Lembrete:
**NÃO faça suposições baseadas em conhecimento externo!**

RETORNE APENAS O JSON, SEM EXPLICAÇÕES
"""

query_relacao = """
with
    source_data as (
        select
            *
        from `__project_id__.__dataset_id__.__table_id_enriquecido__`
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
            a.logradouro as logradouro_report,
            a.numero_logradouro as numero_logradouro_report,
            a.latitude as latitude_report,
            a.longitude as longitude_report,
            a.main_topic as main_topic_report,
            a.related_topics as related_topics_report,
            a.scope_level as scope_level_report,
            a.scope_level_explanation as scope_level_explanation_report,
            a.threat_level as threat_level_report,
            a.threat_explanation as threat_explanation_report,
            a.title_report,
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
