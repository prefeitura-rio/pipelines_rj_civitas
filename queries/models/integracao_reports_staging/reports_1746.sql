{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    unique_key = 'id_report_original',
    partition_by={
        "field": "data_report",
        "data_type": "datetime",
        "granularity": "month",
    }
    )
}}

WITH chamados AS (
  SELECT
    *
  FROM
    {{ source('chamados_1746', 'chamado') }}
),
orgaos_agg AS (
  SELECT
    id_chamado AS id_report_original,
    ARRAY_AGG(IFNULL(nome_unidade_organizacional, '')) AS orgaos
  FROM
    chamados
  GROUP BY
    id_report_original
),
subtipo_agg AS (
  SELECT
    id_chamado,
    id_tipo,
    ARRAY_AGG(IFNULL(subtipo, '')) AS subtipo
  FROM
    chamados
  GROUP BY
    id_chamado,
    id_tipo
),
tipo_subtipo_agg AS (
  SELECT
    c.id_chamado AS id_report_original,
    ARRAY_AGG(
      STRUCT(
        c.tipo,
        t.subtipo
      )
    ) AS tipo_subtipo
  FROM
    chamados c
  LEFT JOIN
    subtipo_agg t ON c.id_chamado = t.id_chamado AND c.id_tipo = t.id_tipo
  GROUP BY
    id_report_original
),
logradouros AS (
  SELECT DISTINCT
    id_logradouro,
    nome_completo
  FROM {{ source('datario', 'logradouro') }}
)
SELECT
  '1746' as id_source,
  id_chamado AS id_report_original,
  data_inicio AS data_report,
  o.orgaos,
  categoria,
  t.tipo_subtipo,
  descricao,
  l.nome_completo AS logradouro,
  CAST(numero_logradouro AS STRING) AS numero_logradouro,
  latitude,
  longitude
FROM chamados c
LEFT JOIN orgaos_agg o ON c.id_chamado = o.id_report_original
LEFT JOIN tipo_subtipo_agg t ON c.id_chamado = t.id_report_original
LEFT JOIN logradouros l ON c.id_logradouro = l.id_logradouro
JOIN
    (SELECT * FROM {{ source('integracao_reports', 'tipos_interesse_1746') }} WHERE id_tipo IS NOT NULL and interesse = 1) ti
  ON
    c.id_tipo = ti.id_tipo
    AND c.id_subtipo = ti.id_subtipo