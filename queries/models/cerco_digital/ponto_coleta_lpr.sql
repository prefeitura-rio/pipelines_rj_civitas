{{
    config(
        materialized='table'
    )
}}

WITH ranked_equipamentos AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY origem_equipamento, codigo_ponto_coleta, sentido
      ORDER BY codigo_equipamento DESC -- Quanto maior o código do equipamento, mais recente é o registro
    ) AS rn
  FROM {{ ref('equipamento') }}
  WHERE
    origem_equipamento IS NOT NULL AND
    codigo_ponto_coleta IS NOT NULL AND
    sentido IS NOT NULL
),
aggregated_points AS (
  SELECT
    origem_equipamento,
    codigo_ponto_coleta,
    MAX(IF(rn = 1, local, NULL)) AS local,   -- local do equipamento de maior código
    sentido,
    MAX(IF(rn = 1, bairro, NULL)) AS bairro, -- bairro do equipamento de maior código
    ROUND(AVG(latitude), 6) AS latitude,
    ROUND(AVG(longitude), 6) AS longitude,
    LOGICAL_OR(status_ativo) AS status_ativo
  FROM ranked_equipamentos
  GROUP BY
    origem_equipamento,
    codigo_ponto_coleta,
    sentido
)
SELECT
  m.id_ponto_coleta,
  ap.origem_equipamento,
  ap.codigo_ponto_coleta,
  ap.local,
  ap.bairro,
  ap.sentido,
  ap.latitude,
  ap.longitude,
  ap.status_ativo
FROM aggregated_points ap
INNER JOIN {{ ref('mapeamento_id_ponto_coleta') }} m
  ON ap.origem_equipamento = m.origem_equipamento
 AND ap.codigo_ponto_coleta = m.codigo_ponto_coleta
 AND ap.sentido = m.sentido
