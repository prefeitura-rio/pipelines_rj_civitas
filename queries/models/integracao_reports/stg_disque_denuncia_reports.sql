{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH orgaos_expanded AS (
  SELECT DISTINCT
    id_denuncia,
    orgao.nome AS nome_orgao
  FROM `rj-civitas.disque_denuncia.denuncias`,
  UNNEST(orgaos) AS orgao
),
orgaos_array AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(STRUCT(nome_orgao AS nome)) AS orgaos
  FROM orgaos_expanded
  GROUP BY id_denuncia
),
assuntos_expanded AS (
  SELECT
    id_denuncia,
    assunto.id_classe AS id_tipo,
    assunto.classe AS tipo,
    assunto.id_tipo AS id_subtipo,
    assunto.tipo AS subtipo
  FROM `rj-civitas.disque_denuncia.denuncias`,
  UNNEST(assuntos) AS assunto
),
subtipo_agg AS (
  SELECT
    id_denuncia,
    id_tipo,
    ARRAY_AGG(DISTINCT subtipo) AS subtipo
  FROM
    assuntos_expanded
  GROUP BY
    id_denuncia,
    id_tipo
),
assuntos_agg AS (
  SELECT
    a.id_denuncia,
    ARRAY_AGG(
      STRUCT(a.tipo, s.subtipo)
      ORDER BY a.id_tipo
    ) AS tipo_subtipo
  FROM
    (SELECT DISTINCT id_denuncia, id_tipo, tipo FROM assuntos_expanded) a
  LEFT JOIN
    subtipo_agg s
  ON
    a.id_denuncia = s.id_denuncia
    AND a.id_tipo = s.id_tipo
  GROUP BY
    a.id_denuncia
)
SELECT
  'DD' AS id_source,
  d.id_denuncia AS id_report_original,
  d.data_denuncia AS data_report,
  o.orgaos,
  'Denúncia' AS categoria,
  a.tipo_subtipo,
  d.relato AS descricao,
  CONCAT(d.tipo_logradouro, ' ', d.logradouro) AS logradouro,
  d.numero_logradouro,
  d.latitude,
  d.longitude
FROM `rj-civitas.disque_denuncia.denuncias` d
LEFT JOIN orgaos_array o ON d.id_denuncia = o.id_denuncia
LEFT JOIN assuntos_agg a ON d.id_denuncia = a.id_denuncia