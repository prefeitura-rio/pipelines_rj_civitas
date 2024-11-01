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
        from `rj-civitas.integracao_reports.reports`  -- tablesample system(10 percent)
        where
            -- datetime(data_report, 'America/Sao_Paulo') >= timestamp_sub(
            -- datetime(current_timestamp(), 'America/Sao_Paulo'), interval 30 minute
            -- )
            -- and id_source = 'DD'
            id_report in (
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
                "DD2666672"  -- Crimes Violentos
            -- "DD2666686",  -- maus tratos contra animais
            -- "DD2666581",  -- Crimes contra o meio ambiente
            -- "DD2666769",  -- maus tratos contra animais
            -- "DD2666645"  -- Lixo acumulado
            )
    -- LIMIT 10
    ),

    prompt_table as (
        select
            *,
            concat(
                '''
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
    "predicted_time_interval": "valor horário previsto",

    "threat_explanation": "Avaliação detalhada da ameaça",
    "threat_level": "valor nível de ameaça"
}

Lembrete: Complete todas as justificativas com base em dados observáveis e use exemplos práticos se possível para reforçar a coerência na análise.


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

    enriched_occurrences as (
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
            prompt as prompt_first_llm,
            ml_generate_text_llm_result
        from llm_response
    ),

    filtered_occurrences as (
        select
            a.id_report,
            a.id_source,
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
                a.data_report, interval cast(a.predicted_time_interval as int64) minute
            ) as predicted_end_time_report,
            a.predicted_time_explanation as predicted_time_explanation_report,
            a.prompt_first_llm,
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
            ifnull(a.data_report, cast('' as datetime)) as data_report_tz,
            ifnull(
                parse_datetime('%d/%m/%Y %H:%M:%S', b.datahora_inicio),
                cast('' as datetime)
            ) as data_inicio_tz
        from enriched_occurrences a
        cross join (select * from `rj-civitas-dev.g20.contextos`) b
        where
            a.data_report >= parse_datetime('%d/%m/%Y %H:%M:%S', b.datahora_inicio)
            and a.data_report <= parse_datetime('%d/%m/%Y %H:%M:%S', b.datahora_fim)
            and lower(a.threat_level) = 'alto'
            and if(
                (a.latitude = 0.0 or a.latitude is null)
                or (a.longitude = 0.0 or a.longitude is null)
                or (b.geometria is null),
                true,
                st_intersects(
                    st_buffer(
                        st_geogfromtext(b.geometria), coalesce(b.raio_de_busca, 5000)  -- RAIO PADRAO DE 5km
                    ),
                    st_geogpoint(a.longitude, a.latitude)
                )
            )
    ),

    prompt_context as (
        select
            *,
            concat(
                '''
Você é um analista de segurança especializado no evento do G20.
Sua função é definir se existe relação entre a ocorrencia e o contexto fornecido.

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
- **Raciocínio**: Oferecer uma explicação detalhada da relação (ou ausência dela) fornece clareza e transparência ao analista. Para justificar de forma prática, exemplos de situações similares e dados específicos da ocorrência e do contexto ajudam a solidificar a análise.
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

ID da Ocorrência: ''',
                id_report,
                '''

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
  'id_report': 'ID da ocorrencia',
  'relation_explanation':'explicacao detalhada do motivo da relacao entre a ocorrencia e o contexto',
  'relation_key_factors' ['lista de fatores que indica a relação entre o contexto e a ocorrencia'],
  'relation_confidence': 'nivel de semelhança entre o contexto e a ocorrencia. Valor entre 0 e 1',
  'relation':'valor da relacao. true/false'
}

Lembrete: Complete todas as justificativas com base em dados observáveis e use exemplos práticos se possível para reforçar a coerência na análise.


RETORNE APENAS O JSON, SEM EXPLICAÇÕES
'''
            ) as prompt
        from filtered_occurrences
    ),

    llm_context_response as (
        select
            *,
            regexp_replace(
                ml_generate_text_llm_result, r"^```json\n|\n```$", ""
            ) json_res_context
        from
            ml.generate_text(
                model `rj-civitas.llm_models.gemini_1_5_flash_002`,
                table prompt_context,
                struct(
                    0.2 as temperature,
                    1024 as max_output_tokens,
                    true as flatten_json_output
                )
            )
    )
select
    * except (
        prompt_first_llm,
        prompt,
        json_res_context,
        ml_generate_text_llm_result,
        ml_generate_text_rai_result,
        ml_generate_text_status
    ),
    array(
        select replace(topic, '"', '')
        from
            unnest(
                json_extract_array(json_res_context, "$.relation_key_factors")
            ) as topic
    ) as relation_key_factors,
    json_extract_scalar(
        json_res_context, "$.relation_confidence"
    ) as relation_confidence,
    json_extract_scalar(
        json_res_context, "$.relation_explanation"
    ) as relation_explanation,
    json_extract_scalar(json_res_context, "$.relation") as relation,
    prompt_first_llm,
    prompt as prompt_second_llm
from llm_context_response
