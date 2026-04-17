{{
    config(
        materialized='table',
        partition_by={
            "field": "origem_equipamento",
            "data_type": "string"
        }
    )
}}

WITH cetrio_radars AS (
  SELECT
      'CETRIO' AS origem_equipamento,
      codcet AS codigo_equipamento,
      codigo_ponto_coleta,
      logradouro AS local,
      sentido,
      bairro,
      latitude,
      longitude,
      ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64), SAFE_CAST(latitude AS FLOAT64)) AS geography,
      IF(status = 'OPERANDO', true, false) AS status_ativo
    FROM {{ ref('radar') }}
    -- Excluindo pontos de coleta problemáticos e que não têm dados (desde 2024)
    WHERE codigo_ponto_coleta NOT IN ('Av Sernambetiba PxSemaforo7627-St Pontal') 
),
civitas_lpr AS (
  SELECT
      'CIVITAS' AS origem_equipamento,
      a.id_camera AS codigo_equipamento,
      a.codigo_ponto_coleta AS codigo_ponto_coleta,
      a.local_ponto_coleta AS local,
      a.sentido AS sentido,
      b.nome AS bairro,
      a.latitude,
      a.longitude,
      ST_GEOGPOINT(SAFE_CAST(a.longitude AS FLOAT64), SAFE_CAST(a.latitude AS FLOAT64)) AS geography,
      status AS status_ativo
    {# FROM {{ source('lpr', 'cameras') }} a #}
    FROM rj-civitas-dev.lpr.cameras a
    LEFT JOIN {{ source('datario', 'bairro') }} b ON ST_WITHIN(ST_GEOGPOINT(a.longitude, a.latitude), b.geometry)
),
all_equipments AS (
    SELECT * FROM cetrio_radars

    UNION ALL

    SELECT * FROM civitas_lpr
),
all_equipments_cleaned AS (
    SELECT
        origem_equipamento,
        codigo_equipamento,
        -- Remove todos os espaços em branco do código do ponto de coleta
        TRIM(REGEXP_REPLACE(codigo_ponto_coleta, r'\s+', '')) AS codigo_ponto_coleta,
        -- Remove espaços extras (mais de um espaço) e substitui por um espaço simples
        UPPER(TRIM(REGEXP_REPLACE(local, r'\s+', ' '))) AS local,
        UPPER(TRIM(REGEXP_REPLACE(sentido, r'\s+', ' '))) AS sentido,
        UPPER(TRIM(REGEXP_REPLACE(bairro, r'\s+', ' '))) AS bairro,
        latitude,
        longitude,
        geography,
        status_ativo
    FROM all_equipments
)
SELECT * FROM all_equipments_cleaned
