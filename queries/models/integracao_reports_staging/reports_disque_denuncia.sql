{{
  config(
    materialized = 'table',
    partition_by={
        "field": "data_report",
        "data_type": "datetime",
        "granularity": "month",
    }
    )
}}

WITH orgaos_expanded AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(UPPER(IFNULL(orgao.nome, ''))) AS orgaos
  FROM `rj-civitas.disque_denuncia.denuncias`,
  UNNEST(orgaos) AS orgao
  GROUP BY id_denuncia
),
tipos_agg AS (
  SELECT
    d.id_denuncia,
    c.id_classe,
    ARRAY_AGG(LOWER(IFNULL(t.tipo, ''))) AS subtipo
  FROM `rj-civitas.disque_denuncia.denuncias` d,
  UNNEST(assuntos) c,
  UNNEST(c.tipos) t
  GROUP BY
    id_denuncia,
    id_classe
),
assuntos_expanded AS (
  SELECT
    d.id_denuncia,
    ARRAY_AGG(STRUCT(LOWER(c.classe) AS tipo, t.subtipo)) AS tipo_subtipo
  FROM `rj-civitas.disque_denuncia.denuncias` d,
  UNNEST(assuntos) AS c
  LEFT JOIN
    tipos_agg t ON d.id_denuncia = t.id_denuncia AND c.id_classe = t.id_classe

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
  INITCAP(CONCAT(d.tipo_logradouro, ' ', d.logradouro)) AS logradouro,
  d.numero_logradouro,
  d.latitude,
  d.longitude
FROM `rj-civitas.disque_denuncia.denuncias` d
LEFT JOIN orgaos_expanded o ON d.id_denuncia = o.id_denuncia
LEFT JOIN assuntos_expanded a ON d.id_denuncia = a.id_denuncia