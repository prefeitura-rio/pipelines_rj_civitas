{{
    config(
        materialized='table',
        cluster_by = ["id_ponto_coleta", "codigo_equipamento"]
    )
}}
-- Uma linha por equipamento com o id_ponto_coleta resolvido (chave de negócio → ID interno).
SELECT
  m.id_ponto_coleta,
  e.origem_equipamento,
  e.codigo_equipamento,
  e.codigo_ponto_coleta,
  e.sentido,
  e.local,
  e.bairro,
  e.latitude,
  e.longitude,
  e.geography,
  e.status_ativo
FROM {{ ref('equipamento') }} AS e
INNER JOIN {{ ref('mapeamento_id_ponto_coleta') }} AS m
  ON e.origem_equipamento = m.origem_equipamento
 AND e.codigo_ponto_coleta = m.codigo_ponto_coleta
 AND e.sentido = m.sentido
