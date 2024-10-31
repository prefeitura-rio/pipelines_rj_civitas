-- CREATE OR REPLACE TABLE `rj-civitas-dev.g20.reports_enriquecidos_v2` AS (
-- SELECT *
-- FROM `rj-civitas-dev.g20_views.reports_enriquecidos_v2` src
-- )
-- INSERT INTO `rj-civitas-dev.g20.reports_enriquecidos_v2`
-- SELECT
-- *,
-- FROM (
-- SELECT *
-- FROM `rj-civitas-dev.g20_views.reports_enriquecidos_v2` src
-- WHERE NOT EXISTS (
-- SELECT 1
-- FROM `rj-civitas-dev.g20.reports_enriquecidos_v2` AS target
-- WHERE target.id = src.id
-- )
-- )
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
        from `rj-civitas.integracao_reports.reports`
        where
            datetime(data_report, 'America/Sao_Paulo') >= timestamp_sub(
                datetime(current_timestamp(), 'America/Sao_Paulo'), interval 30 minute
            )
            and id_source = 'DD'
    {# and (latitude is not null or longitude is not null) #}
    ),

    prompt_table as (
        select
            *,
            concat(
                '''Dado este texto sobre uma ocorrência, determine: 
        1. O tópico principal e quaisquer tópicos relacionados com base nas categorias e descrição fornecidas 
        2. O nível de abrangência apropriado (Casa, Quadra, Bairro, Região da Cidade, Cidade, Estado, País) 
        3. O nível de urgência (Nenhuma, Baixa, Média, Alta) da ocorrência que descreve o potencial de escalada do problema caso não seja tratado. A urgencia está relacionada com o nível de risco a vida das pessoas ao redor.
        4. Prever horários de início e fim com base na data do relatório e no desenrolar previsto do contexto. 
        Texto: 
        ID da Ocorrência: ''',
                id_report,
                ''' 
        ID da Fonte: ''',
                id_source,
                ''' 
        ID Original: ''',
                id_report_original,
                ''' 
        Data do Relatório (Quando a denuncia chegou a prefeitura): ''',
                cast(data_report as string),
                ''' 
        Categoria: ''',
                categoria,
                ''' 
        Tipo/Subtipo: ''',
                to_json_string(tipo_subtipo),
                ''' 
        Descrição: ''',
                descricao,
                ''' 
        Organizações: ''',
                to_json_string(orgaos),
                ''' 
        Endereço: ''',
                logradouro,
                ''', ''',
                numero_logradouro,
                ''' 
        Localização: ''',
                latitude,
                ''', ''',
                longitude,
                ''' 
        Retorne apenas os seguintes campos em JSON: 
        { 
            "id_report": "ID da ocorrência", 
            "main_topic": "tópico principal", 
            "related_topics": ["array de tópicos relacionados"], 
            "scope_level_explanation": "Explicacão para o nível de abrangência"
            "scope_level": "nível de abrangência", 
            "urgenct_explanation": "Explicação para o nível de urgência",
            "urgency": "nível de urgência", 
            "predicted_times_explanation": "Explicação para as datas previstas",
            "predicted_start_time": "timestamp ISO", 
            "predicted_end_time": "timestamp ISO", 
        } 
        RETORNE APENAS O JSON, SEM EXPLICAÇÕES '''
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
                model `rj-civitas.scraping_redes_staging.llm_gemini_1_5_flash_model`,
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
            json_extract_scalar(json_res, "$.id_report") as id_report_llm,
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
            json_extract_scalar(json_res, "$.main_topic") as main_topic,
            array(
                select replace(topic, '"', '')
                from unnest(json_extract_array(json_res, "$.related_topics")) as topic
            ) as related_topics,
            json_extract_scalar(
                json_res, "$.scope_level_explanation"
            ) as scope_level_explanation,
            json_extract_scalar(json_res, "$.scope_level") as scope_level,
            json_extract_scalar(
                json_res, "$.urgenct_explanation"
            ) as urgenct_explanation,
            json_extract_scalar(json_res, "$.urgency") as urgency,
            json_extract_scalar(
                json_res, "$.predicted_times_explanation"
            ) as predicted_times_explanation,
            data_report,
            json_extract_scalar(
                json_res, "$.predicted_start_time"
            ) as predicted_start_time,
            json_extract_scalar(json_res, "$.predicted_end_time") as predicted_end_time,
            prompt,
            ml_generate_text_llm_result
        from llm_response
    )

select *
from src
