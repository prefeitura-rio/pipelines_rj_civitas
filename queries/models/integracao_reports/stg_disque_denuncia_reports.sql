{{
  config(
    materialized = 'ephemeral',
    )
}}

WITH orgaos_expanded AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(STRUCT(orgao.nome AS nome)) AS orgaos
  FROM `rj-civitas.disque_denuncia.denuncias`,
  UNNEST(orgaos) AS orgao
  GROUP BY id_denuncia
),
assuntos_expanded AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(STRUCT(assunto.classe AS tipo, assunto.tipo AS subtipo)) AS tipo_subtipo
  FROM `rj-civitas.disque_denuncia.denuncias`,
  UNNEST(assuntos) AS assunto
  GROUP BY id_denuncia
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
LEFT JOIN orgaos_expanded o ON d.id_denuncia = o.id_denuncia
LEFT JOIN assuntos_expanded a ON d.id_denuncia = a.id_denuncia
