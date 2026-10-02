{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['origem_equipamento', 'codigo_ponto_coleta', 'sentido'],
        cluster_by=['origem_equipamento', 'codigo_ponto_coleta', 'sentido'],
        merge_exclude_columns=['created_at', 'id_ponto_coleta']
    )
}}

-- Tabela de mapeamento (append-like): (origem_equipamento, codigo_ponto_coleta, sentido) -> id_ponto_coleta
-- Objetivo: preservar um ID interno estável para cada chave de negócio, enquanto atributos podem mudar em outro modelo.
WITH pontos_com_duplicatas as (
  SELECT codigo_ponto_coleta AS ponto_duplicado, COUNT(DISTINCT CONCAT(sentido, bairro, local)) qtd
  FROM {{ ref('equipamento')}}
  WHERE status_ativo = TRUE
  GROUP BY codigo_ponto_coleta
  HAVING qtd > 1
),
latlong_distante AS (
    SELECT 
      codigo_ponto_coleta AS codigo_latlong_distante,
      geography,
      LAG(geography) OVER (PARTITION BY codigo_ponto_coleta ORDER BY latitude) AS lag_geo 
    FROM {{ ref('equipamento') }}
    WHERE status_ativo = TRUE
    QUALIFY ST_DISTANCE(geography, lag_geo) > 1000
),
point_keys AS (
  SELECT DISTINCT
    a.origem_equipamento,
    a.codigo_ponto_coleta,
    a.sentido
  FROM {{ ref('equipamento') }} a
  LEFT JOIN pontos_com_duplicatas b
  ON a.codigo_ponto_coleta = b.ponto_duplicado
  LEFT JOIN latlong_distante c
  ON a.codigo_ponto_coleta = c.codigo_latlong_distante
  WHERE
    b.ponto_duplicado IS NULL
    AND c.codigo_latlong_distante IS NULL
    AND a.origem_equipamento IN ('CETRIO', 'CIVITAS')
    AND COALESCE(a.codigo_ponto_coleta, '') != ''
    AND a.sentido IS NOT NULL
    AND a.latitude BETWEEN -90 AND 0
    AND a.longitude BETWEEN -90 AND 0
    AND a.status_ativo IS NOT NULL
    AND a.bairro IS NOT NULL
),
point_key_id_map AS (
  {% if is_incremental() %}
  -- No incremental, só inserimos chaves ainda não presentes no destino (comportamento append-like).
  WITH existing_point_keys AS (
    SELECT DISTINCT
      origem_equipamento,
      codigo_ponto_coleta,
      sentido
    FROM {{ this }}
  ),
  new_point_keys AS (
    SELECT
      pk.*
    FROM point_keys pk
    LEFT JOIN existing_point_keys epk
      ON pk.origem_equipamento = epk.origem_equipamento
     AND pk.codigo_ponto_coleta = epk.codigo_ponto_coleta
     AND pk.sentido = epk.sentido
    WHERE epk.codigo_ponto_coleta IS NULL
  ),
  -- Base da sequência incremental: próximo ID começa em max(id_ponto_coleta) + 1.
  current_max_id AS (
    SELECT
      COALESCE(MAX(SAFE_CAST(id_ponto_coleta AS INT64)), 0) AS max_id_ponto_coleta
    FROM {{ this }}
  )
  SELECT
    LPAD(
      CAST(
        current_max_id.max_id_ponto_coleta
        + ROW_NUMBER() OVER (
          -- Ordem determinística para garantir repetibilidade dentro do mesmo lote.
          ORDER BY FARM_FINGERPRINT(CONCAT(npk.codigo_ponto_coleta, npk.origem_equipamento, npk.sentido))
        ) AS STRING
      ),
      7,
      '0'
    ) AS id_ponto_coleta,
    npk.origem_equipamento,
    npk.codigo_ponto_coleta,
    npk.sentido
  FROM new_point_keys npk
  CROSS JOIN current_max_id
  {% else %}
  SELECT
    -- Full-refresh: recria a numeração inteira a partir de 0000001.
    LPAD(
      CAST(
        ROW_NUMBER() OVER (
          ORDER BY FARM_FINGERPRINT(CONCAT(codigo_ponto_coleta, origem_equipamento, sentido))
        ) AS STRING
      ),
      7,
      '0'
    ) AS id_ponto_coleta,
    origem_equipamento,
    codigo_ponto_coleta,
    sentido
  FROM point_keys
  {% endif %}
)
SELECT
  id_ponto_coleta,
  origem_equipamento,
  codigo_ponto_coleta,
  sentido,
  CURRENT_TIMESTAMP() AS created_at,
  CURRENT_TIMESTAMP() AS updated_at
FROM point_key_id_map

