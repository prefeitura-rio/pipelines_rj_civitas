
WITH
rj_geo AS (
  SELECT
    ST_Union(ARRAY_AGG(geometry)) AS city_geometry
  FROM
    {{ source('datario', 'bairro') }}
),
occurrences AS (
  SELECT
    a.id_report,
    a.id_source,
    a.id_report_original,
    a.data_report,
    a.orgaos AS orgaos_report,
    a.categoria AS categoria_report,
    a.tipo_subtipo AS tipo_subtipo_report ,
    a.descricao AS descricao_report,
    a.latitude AS latitude_report,
    a.longitude AS longitude_report,
    a.scope_level AS scope_level_report,
    a.scope_level_explanation AS scope_level_explanation_report,
    a.urgency AS urgency_report,
    a.urgenct_explanation AS urgency_explanation_report,
    a.predicted_start_time AS predicted_start_time_report,
    a.predicted_end_time AS predicted_end_time_report,
    a.predicted_times_explanation AS predicted_times_explanation_report,
    b.id AS id_contexto,
    b.tipo AS tipo_contexto,
    b.datahora_inicio AS datahora_inicio_contexto,
    b.datahora_fim AS datahora_fim_contexto,
    b.nome AS nome_contexto,
    b.descricao AS descricao_contexto,
    b.informacoes_adicionais AS informacoes_adicionais_contexto,
    b.endereco AS endereco_contexto,
    b.local AS local_contexto,
    b.geometria AS geometria_contexto,
    b.raio_de_busca AS raio_de_busca_contexto,
    a.data_report AS data_report_tz,
    PARSE_DATETIME('%d/%m/%Y %H:%M:%S', b.datahora_inicio) AS data_inicio_tz
FROM
    {{ ref('reports_enriquecidos_v2') }}
CROSS JOIN
  (
    SELECT
      *
    FROM
      {{ source('g20', 'contextos') }}
  ) b
WHERE
  a.data_report >= PARSE_DATETIME('%d/%m/%Y %H:%M:%S', b.datahora_inicio)
  AND a.data_report <= PARSE_DATETIME('%d/%m/%Y %H:%M:%S', b.datahora_fim)
  AND LOWER(a.urgency) != 'nenhuma'
  AND ST_INTERSECTS(
    ST_BUFFER(
      COALESCE(ST_GEOGFROMTEXT(b.geometria), (SELECT city_geometry FROM rj_geo)), -- CIDADE INTEIRA, CASO geometria IS NULL
      COALESCE(b.raio_de_busca, 5000) -- RAIO PADRAO DE 5km
    ),
    ST_GEOGPOINT(a.longitude, a.latitude)
  )
)
SELECT
  *
FROM
  occurrences
