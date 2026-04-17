-- Verifica se há gaps no id_ponto_coleta da tabela map_ponto_coleta
WITH gaps AS (
  SELECT
    CAST(id_ponto_coleta AS INT64) AS id_atual,
    CAST(LAG(id_ponto_coleta) OVER (ORDER BY CAST(id_ponto_coleta AS INT64)) AS INT64) AS id_anterior
  FROM {{ ref('mapeamento_id_ponto_coleta') }}
)
SELECT *
FROM gaps
WHERE id_atual - id_anterior > 1