{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        unique_key='id',
        partition_by={
            "field": "data_dia",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ['placa']
    )
}}

WITH leituras AS (
  SELECT
    datahora,
    REGEXP_REPLACE(UPPER(placa), r'[^A-Z0-9]', '') AS placa,
    SAFE_CAST(id_ponto_coleta AS STRING) AS id_ponto_coleta,
    SAFE_CAST(camera_numero AS STRING) AS camera_numero,
    camera_latitude AS latitude,
    camera_longitude AS longitude
  FROM {{ ref('vw_all_readings') }}
  WHERE datahora >= TIMESTAMP({{ var("start_date") }}, 'America/Sao_Paulo')
    AND datahora < TIMESTAMP({{ var("end_date") }}, 'America/Sao_Paulo')
    AND placa IS NOT NULL
    AND camera_latitude IS NOT NULL
    AND camera_longitude IS NOT NULL
    AND camera_latitude < 0
    AND camera_longitude < 0
),

leituras_validas AS (
  SELECT
    *,
    COUNT(*) OVER (PARTITION BY placa) AS total_leituras_placa
  FROM leituras
  WHERE REGEXP_CONTAINS(placa, r'^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$')  -- TODO: Talvez precise duplicar as chaves ex:{{3}} para escapar da sintaxe jinja
    AND id_ponto_coleta != '949' -- TODO: Tirar esses filtros manuais de câmera inválida
    AND camera_numero != '0530511121' -- TODO: Tirar esses filtros manuais de câmera inválida
),

leituras_pares AS (
  SELECT
    placa,
    total_leituras_placa,
    datahora AS datahora_b,
    id_ponto_coleta AS ponto_b,
    camera_numero AS camera_b,
    latitude AS latitude_b,
    longitude AS longitude_b,
    LAG(datahora) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS datahora_a,
    LAG(id_ponto_coleta) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS ponto_a,
    LAG(camera_numero) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS camera_a,
    LAG(latitude) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS latitude_a,
    LAG(longitude) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS longitude_a
  FROM leituras_validas
),

pares_consecutivos AS (
  SELECT
    placa,
    DATE(datahora_a, 'America/Sao_Paulo') AS data_dia,
    datahora_a,
    datahora_b,
    ponto_a,
    ponto_b,
    camera_a,
    camera_b,
    ST_GEOGPOINT(longitude_a, latitude_a) AS geolocation_a,
    ST_GEOGPOINT(longitude_b, latitude_b) AS geolocation_b,
    ROUND(SAFE_DIVIDE(
      ST_DISTANCE(
        ST_GEOGPOINT(longitude_a, latitude_a),
        ST_GEOGPOINT(longitude_b, latitude_b)
      ),
      1000.0
    ), 2) AS distancia_km,
    ROUND(SAFE_DIVIDE(TIMESTAMP_DIFF(datahora_b, datahora_a, SECOND), 3600.0), 2) AS delta_horas,
    total_leituras_placa
  FROM leituras_pares
  WHERE datahora_a IS NOT NULL
    AND TIMESTAMP_DIFF(datahora_b, datahora_a, SECOND) > 0
    AND ponto_a IS NOT NULL
    AND ponto_a != ponto_b
),
pares_suspeitos_validados AS (
  SELECT
    placa,
    data_dia,
    total_leituras_placa,
    datahora_a,
    datahora_b,
    ponto_a,
    ponto_b,
    camera_a,
    camera_b,
    geolocation_a,
    geolocation_b,
    distancia_km,
    delta_horas,
    ROUND(SAFE_DIVIDE(distancia_km, delta_horas), 2) AS velocidade_implicita_kmh
  FROM pares_consecutivos
  WHERE distancia_km >= {{ var('distance_threshold_km') }}
    AND ROUND(SAFE_DIVIDE(distancia_km, delta_horas), 2) >= {{ var('speed_threshold_kmh') }}
)

SELECT
  CONCAT(placa, data_dia) AS id,
  placa,
  data_dia,
  total_leituras_placa,
  ARRAY_AGG(
      STRUCT(
        datahora_a,
        ponto_a,
        camera_a,
        geolocation_a,
        datahora_b,
        ponto_b,
        camera_b,
        geolocation_b,
        distancia_km,
        delta_horas,
        velocidade_implicita_kmh
      )
      ORDER BY datahora_a
  ) AS pares_suspeitos,
  ROUND(MAX(distancia_km), 2) AS distancia_maxima,
  ROUND(MIN(distancia_km), 2) AS distancia_minima,
  ROUND(AVG(velocidade_implicita_kmh), 2) AS velocidade_implicita_media,
  ROUND(MAX(velocidade_implicita_kmh), 2) AS velocidade_implicita_maxima
  FROM pares_suspeitos_validados
  GROUP BY placa, data_dia, total_leituras_placa
  HAVING ARRAY_LENGTH(pares_suspeitos) >= {{ var('min_pairs_per_day') }}
