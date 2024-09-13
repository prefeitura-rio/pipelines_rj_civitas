{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id_ocorrencia',
        partition_by={
            "field": "data_ocorrencia",
            "data_type": "datetime",
            "granularity": "month",
        }
    )
}}
WITH row_number_data AS (
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY id ORDER BY timestamp_insercao) rn
  FROM
    {{ source('fogo_cruzado', 'ocorrencias') }}
),
newest_data AS (
  SELECT
    * EXCEPT(rn)
  FROM
    row_number_data
  WHERE
    rn = 1
   {% if is_incremental() %}
     AND timestamp_insercao > (SELECT MAX(timestamp_insercao) FROM {{ this }})
   {% endif %}
 ),
complementary_reasons_agg AS (
  SELECT
    o.id,
    ARRAY_AGG(
        c.name
    ) AS motivos_complementares
  FROM
    newest_data o,
    UNNEST(contextInfo.complementaryReasons) c
  GROUP BY
  id
),
clippings_agg AS (
  SELECT
    o.id,
    ARRAY_AGG(
        c.name
    ) AS categorias
  FROM
    newest_data o,
    UNNEST(contextInfo.clippings) c
  GROUP BY
  id
),
transports_agg AS (
  SELECT
    o.id,
    ARRAY_AGG(
      STRUCT(
        c.transport.name AS tipo_transporte,
        c.interruptedTransport AS interrupcao_transporte,
        c.dateInterruption AS data_interrupcao_transporte,
        c.releaseDate AS data_retomada_transporte,
        c.transportDescription AS descricao_interrupcao_transporte
      )
    ) AS transportes
  FROM
    newest_data o,
    UNNEST(transports) c
  GROUP BY
    id
),
victims_circumstances_agg AS (
  SELECT
    o.id,
    v.id AS id_vitima,
    ARRAY_AGG(
    STRUCT(
      vc.name AS descricao_circunstancia,
      vc.type AS tipo_circunstancia
    )
  ) circunstancias_vitima
  FROM
    newest_data o,
    UNNEST(victims) v,
    UNNEST(v.circumstances) vc
  GROUP BY
    id,
    id_vitima
),
victims_qualifications AS (
  SELECT
    o.id,
    v.id id_vitima,
    ARRAY_AGG(
      STRUCT(
        q.name AS qualificacao_vitima,
        q.type AS tipo_qualificacao_vitima
      )
    ) AS qualificacoes_vitima
  FROM
    newest_data o
    , UNNEST(victims) v
    , UNNEST(qualifications) q
  GROUP BY
    id,
    id_vitima
),
victims_agg AS (
  SELECT
    o.id,
    ARRAY_AGG(
      STRUCT(
        v.id AS id_vitima,
        v.situation AS situacao_vitima,
        avc.circunstancias_vitima,
        v.deathDate AS data_morte_vitima,
        v.age AS idade_vitima,
        v.ageGroup.name AS faixa_etaria_vitima,
        v.genre.name AS genero_vitima,
        v.race AS raca_vitima,
        v.place.name AS local_ocorrencia,
        v.serviceStatus.name AS status_servico,
        v.serviceStatus.type AS tipo_servico,
        qu.qualificacoes_vitima,
        v.politicalPosition.name AS posicao_politica_vitima,
        v.politicalPosition.type AS tipo_posicao_politica_vitima,
        v.politicalStatus.name AS status_politico_vitima,
        v.politicalStatus.type AS tipo_status_politico_vitima,
        v.partie.name AS partido_politico_vitima,
        v.coorporation.name AS corporacao_vitima,
        v.agentPosition.name AS patente_vitima,
        v.agentPosition.type AS tipo_patente_vitima,
        v.agentStatus.name AS status_agente_vitima,
        v.unit AS unidade_policial_vitima
      )
    ) AS vitimas
  FROM
    newest_data o
    , UNNEST(victims) v
  LEFT JOIN victims_circumstances_agg avc ON o.id = avc.id AND v.id = avc.id_vitima
  LEFT JOIN victims_qualifications qu ON o.id = qu.id AND v.id = qu.id_vitima
  GROUP BY
    id
),
animal_victims_circumstances_agg AS (
  SELECT
    o.id,
    av.id AS id_animal_vitima,
    ARRAY_AGG(
      STRUCT(
        avc.name AS descricao_circunstancia,
        avc.type AS tipo_circunstancia
      )
    ) circunstancias_animal
  FROM
    newest_data o,
    UNNEST(animalVictims) av,
    UNNEST(av.circumstances) avc
  GROUP BY
    id,
    id_animal_vitima
),
animal_victims_agg AS (
  SELECT
    o.id,
    ARRAY_AGG(
      STRUCT(
        av.name AS nome_animal,
        av.animalType.type AS tipo_animal,
        av.situation AS situacao_animal,
        avc.circunstancias_animal,
        av.deathDate AS data_morte_animal
      )
    ) AS vitimas_animais
  FROM
    newest_data o,
    UNNEST(animalVictims) av
    LEFT JOIN UNNEST(circumstances) c ON TRUE
  LEFT JOIN animal_victims_circumstances_agg avc ON o.id = avc.id AND av.id = avc.id_animal_vitima
  GROUP BY
    id
), final_query AS  (
SELECT
  oc.id AS id_ocorrencia,
  documentNumber AS numero_ocorrencia,
  address AS endereco,
  state.name AS estado,
  region.region AS regiao,
  city.name AS cidade,
  neighborhood.name AS bairro,
  subNeighborhood.name AS subbairro,
  locality.name AS localidade,
  latitude,
  longitude,
  DATETIME(`date`, 'America/Sao_Paulo') AS data_ocorrencia,
  policeAction AS acao_policial,
  agentPresence AS presenca_agente_seguranca,
  relatedRecord AS ocorrencia_relacionada,
  contextInfo.mainReason.name AS motivo_principal,
  cr.motivos_complementares,
  cl.categorias,
  contextInfo.massacre AS massacre,
  contextInfo.policeUnit AS unidade_policial,
  tr.transportes,
  vi.vitimas,
  av.vitimas_animais,
  timestamp_insercao
FROM
  newest_data oc
LEFT JOIN complementary_reasons_agg cr ON oc.id = cr.id
LEFT JOIN clippings_agg cl ON oc.id = cl.id
LEFT JOIN transports_agg tr ON oc.id = tr.id
LEFT JOIN animal_victims_agg av ON oc.id = av.id
LEFT JOIN victims_agg vi ON oc.id = vi.id
)
SELECT * FROM final_query

