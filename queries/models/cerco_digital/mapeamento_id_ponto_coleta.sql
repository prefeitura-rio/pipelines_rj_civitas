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
WITH point_keys AS (
  SELECT DISTINCT
    origem_equipamento,
    codigo_ponto_coleta,
    sentido
  FROM {{ ref('equipamento') }}
  WHERE
    origem_equipamento IS NOT NULL
    AND codigo_ponto_coleta IS NOT NULL
    AND sentido IS NOT NULL
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

