{{
    config(
        materialized='table'
    )
}}

WITH pontos_com_duplicatas as (
  SELECT codigo_ponto_coleta AS ponto_duplicado, COUNT(DISTINCT CONCAT(sentido, bairro, local)) qtd
  FROM {{ ref('equipamento')}}
  WHERE status_ativo = TRUE AND COALESCE(codigo_ponto_coleta, '') != ''
  GROUP BY codigo_ponto_coleta
  HAVING qtd > 1
),
latlong_distante AS (
    SELECT 
      codigo_ponto_coleta AS codigo_latlong_distante,
      geography,
      LAG(geography) OVER (PARTITION BY codigo_ponto_coleta ORDER BY latitude) AS lag_geo 
    FROM {{ ref('equipamento') }}
    WHERE status_ativo = TRUE AND COALESCE(codigo_ponto_coleta, '') != ''
    QUALIFY ST_DISTANCE(geography, lag_geo) > 1000
),
ranked_equipamentos AS (
  SELECT a.*,
    ROW_NUMBER() OVER (
      PARTITION BY a.origem_equipamento, a.codigo_ponto_coleta, a.sentido
      ORDER BY a.codigo_equipamento DESC -- Quanto maior o código do equipamento, mais recente é o registro
    ) AS rn
  FROM {{ ref('equipamento')}} a
  LEFT JOIN pontos_com_duplicatas b
  ON a.codigo_ponto_coleta = b.ponto_duplicado
  LEFT JOIN latlong_distante c
  ON a.codigo_ponto_coleta = c.codigo_latlong_distante
  WHERE
    b.ponto_duplicado IS NULL AND
    c.codigo_latlong_distante IS NULL AND
    a.codigo_equipamento IS NOT NULL AND
    a.origem_equipamento IN ('CETRIO', 'CIVITAS') AND
    a.codigo_ponto_coleta IS NOT NULL AND
    a.sentido IS NOT NULL AND
    a.latitude BETWEEN -90 AND 0 AND
    a.longitude BETWEEN -90 AND 0 AND
    a.status_ativo IS NOT NULL AND
    a.bairro IS NOT NULL
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
