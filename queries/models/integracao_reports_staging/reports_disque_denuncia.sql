{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id_report_original',
    partition_by={
        "field": "data_report",
        "data_type": "datetime",
        "granularity": "month",
    },
    cluster_by = ["timestamp_insercao"]
    )
}}
WITH denuncias AS (
  SELECT
    *
  FROM
    {{ source('disque_denuncia', 'denuncias') }}

  {% if is_incremental() %}
    WHERE
      timestamp_insercao >= (select max(timestamp_insercao) from {{ this }})
  {% endif %}
),
-- Expand the organs associated with each report and aggregate organ names into an array
orgaos_expanded AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(UPPER(IFNULL(orgao.nome, ''))) orgaos
  FROM denuncias,
  UNNEST(orgaos) AS orgao
  GROUP BY id_denuncia
),
-- Aggregate the subtypes into an array, grouped by report ID and class ID
tipos_agg AS (
  SELECT
    d.id_denuncia,
    c.id_classe,
    ARRAY_AGG(LOWER(IFNULL(t.tipo, ''))) AS subtipo
  FROM denuncias d,
  UNNEST(assuntos) c,
  UNNEST(c.tipos) t
  GROUP BY
    id_denuncia,
    id_classe
),
-- Expand the subtype associated with each report and combine with aggregated types
assuntos_expanded AS (
  SELECT
    d.id_denuncia,
    ARRAY_AGG(STRUCT(LOWER(c.classe) AS tipo, t.subtipo)) AS tipo_subtipo
  FROM denuncias d,
  UNNEST(assuntos) AS c
  LEFT JOIN
    tipos_agg t ON d.id_denuncia = t.id_denuncia AND c.id_classe = t.id_classe

  GROUP BY id_denuncia
)
-- Select final data, joining expanded information and filtering by location
SELECT
  'DD' AS id_source,
  d.id_denuncia AS id_report_original,
  d.data_denuncia AS data_report,
  o.orgaos,
  'Denúncia' AS categoria,
  a.tipo_subtipo,
  d.relato AS descricao,
  INITCAP(CONCAT(d.tipo_logradouro, ' ', d.logradouro)) AS logradouro,
  d.numero_logradouro,
  d.latitude,
  d.longitude,
  d.timestamp_insercao
FROM denuncias d
LEFT JOIN orgaos_expanded o ON d.id_denuncia = o.id_denuncia
LEFT JOIN assuntos_expanded a ON d.id_denuncia = a.id_denuncia
WHERE
  ST_WITHIN(
    ST_GEOGPOINT(
      COALESCE(longitude, -43.5598945),
      COALESCE(latitude, -22.9292129)
    ),
    (SELECT ST_UNION_AGG(geometry) AS city_geometry FROM `datario.dados_mestres.bairro`)
  )
