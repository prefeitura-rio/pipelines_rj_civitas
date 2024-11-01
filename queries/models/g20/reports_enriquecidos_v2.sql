with
    source_reports as (
        select
            ifnull(id_report, '') as id_report,
            ifnull(id_source, '') as id_source,
            ifnull(id_report_original, '') as id_report_original,
            ifnull(
                datetime(data_report, 'America/Sao_Paulo'), cast('' as datetime)
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
        from `rj-civitas.integracao_reports.reports` tablesample system(10 percent)
        where
            datetime(data_report, 'America/Sao_Paulo') >= timestamp_sub(
                datetime(current_timestamp(), 'America/Sao_Paulo'), interval 30 minute
            )
            and id_source = 'DD'
    -- id_report in (
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
    -- "DD2666686",  -- maus tratos contra animais
    -- "DD2666581",  -- Crimes contra o meio ambiente
    -- "DD2666769",  -- maus tratos contra animais
    -- "DD2666645"  -- Lixo acumulado
    -- )
    -- LIMIT 10
    ),

    prompt_table as (
        select
            *,
            concat(
                '''
Você é um analista de segurança especializado no evento do G20. Sua função é avaliar ocorrências e classificar seu risco potencial para a vida e integridade física dos participantes do evento.
Por não conhecer a localização dos eventos não leve em consideração para sua avaliação. Forneça justificativas claras e objetivas para cada classificação.
Você deve preencher todos os campos do JSON

Para cada ocorrência, analise:
1. Tópico principal e tópicos relacionados
2. Nível de abrangência com justificativa
3. Intervalo de tempo previsto com explicação
4. Nível de ameaça com avaliação detalhada

Níveis de Ameaça:
- BAIXO: Risco indireto ou muito improvável (Não representa risco a vida ou integridade, maustratos a animais, transgreção: ambiental, trabalhista, etc)
- ALTO: Ameaça iminente à vida ou integridade física (tiroteios, bloqueio de vias, manifestações, ameaças de bombas ou terrorismo, etc)

Níveis de Escopo:
- Casa: Afeta apenas uma residência/estabelecimento
- Quadra: Impacto limitado à área da quadra
- Bairro: Afeta um bairro inteiro
- Região: Impacta múltiplos bairros
- Cidade: Afeta toda a cidade
- Estado: Impacto em nível estadual
- País: Repercussão nacional

Avaliação Temporal:
- Especificar em minutos o tempo estimado até o possível início do evento
- Usar 0 para casos indefinidos
- Basear estimativa em informações concretas da ocorrência

Ocorrencia :
ID da Ocorrência: ''',
                id_report,
                '''

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
    "predicted_time_interval": "valor horario previsto",

    "threat_explanation": "Avaliação detalhada da ameaça",
    "threat_level": "valor nivel de ameaca",

}


RETORNE APENAS O JSON, SEM EXPLICAÇÕES
'''
            ) as prompt
        from source_reports
    ),

    llm_response as (
        select
            *,
            regexp_replace(
                ml_generate_text_llm_result, r"^```json\n|\n```$", ""
            ) json_res
        from
            ml.generate_text(
                model `rj-civitas.llm_models.gemini_1_5_flash_002`,
                table prompt_table,
                struct(
                    0.2 as temperature,
                    1024 as max_output_tokens,
                    true as flatten_json_output
                )
            )
    ),

    src as (
        select
            farm_fingerprint(concat(id_report, prompt)) as id,
            id_report,
            id_source,
            id_report_original,
            orgaos,
            categoria,
            tipo_subtipo,
            descricao,
            logradouro,
            numero_logradouro,
            latitude,
            longitude,
            array(
                select replace(topic, '"', '')
                from unnest(json_extract_array(json_res, "$.related_topics")) as topic
            ) as related_topics,

            json_extract_scalar(
                json_res, "$.scope_level_explanation"
            ) as scope_level_explanation,
            json_extract_scalar(json_res, "$.scope_level") as scope_level,

            json_extract_scalar(json_res, "$.main_topic") as main_topic,

            json_extract_scalar(json_res, "$.threat_explanation") as threat_explanation,
            json_extract_scalar(json_res, "$.threat_level") as threat_level,

            json_extract_scalar(
                json_res, "$.predicted_time_explanation"
            ) as predicted_time_explanation,
            data_report,
            json_extract_scalar(
                json_res, "$.predicted_time_interval"
            ) as predicted_time_interval,
            prompt,
            ml_generate_text_llm_result
        from llm_response
    )

select *
from src
order by data_report desc
