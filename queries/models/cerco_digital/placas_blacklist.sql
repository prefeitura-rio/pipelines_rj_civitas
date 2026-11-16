{{
    config(
        tags = ['monthly'],
        materialized='incremental',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        unique_key=['placa'],
        cluster_by=['placa']
    )
}}

WITH
  leituras_pares AS (
    SELECT
      placa,
      datahora AS datahora_posterior,
      id_ponto_coleta AS ponto_posterior,
      camera_numero AS camera_posterior,
      camera_latitude AS latitude_posterior,
      camera_longitude AS longitude_posterior,
      LAG(datahora)
        OVER (
          PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
        ) AS datahora_anterior,
      LAG(id_ponto_coleta)
        OVER (
          PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
        ) AS ponto_anterior,
      LAG(camera_numero)
        OVER (
          PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
        ) AS camera_anterior,
      LAG(camera_latitude)
        OVER (
          PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
        ) AS latitude_anterior,
      LAG(camera_longitude)
        OVER (
          PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
        ) AS longitude_anterior
    FROM {{ ref('vw_all_readings_pre_filter') }}
    WHERE
      REGEXP_CONTAINS(placa, r'^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$')
      AND datahora >= TIMESTAMP_SUB(
        TIMESTAMP(CURRENT_DATE('America/Sao_Paulo')), INTERVAL 30 DAY)
  ),
  pares_consecutivos AS (
    SELECT
      placa,
      datahora_anterior,
      datahora_posterior,
      ponto_anterior,
      ponto_posterior,
      camera_anterior,
      camera_posterior,
      ST_GEOGPOINT(longitude_anterior, latitude_anterior)
        AS geolocation_anterior,
      ST_GEOGPOINT(longitude_posterior, latitude_posterior)
        AS geolocation_posterior,
      SAFE_DIVIDE(
        ST_DISTANCE(
          ST_GEOGPOINT(longitude_anterior, latitude_anterior),
          ST_GEOGPOINT(longitude_posterior, latitude_posterior)),
        1000.0) AS distancia_km,
      SAFE_DIVIDE(
        TIMESTAMP_DIFF(datahora_posterior, datahora_anterior, SECOND), 3600.0)
        AS delta_horas
    FROM leituras_pares
    WHERE
      datahora_anterior IS NOT NULL
      AND TIMESTAMP_DIFF(datahora_posterior, datahora_anterior, SECOND) > 0
      AND ponto_anterior IS NOT NULL
      AND ponto_anterior != ponto_posterior
      AND camera_anterior != camera_posterior
  ),
  pares_suspeitos AS (
    SELECT
      CONCAT(
        placa,
        ponto_anterior,
        ponto_posterior,
        CAST(datahora_anterior AS STRING),
        CAST(datahora_posterior AS STRING)) AS id,
      placa,
      DATE(datahora_posterior, 'America/Sao_Paulo') AS data_dia,
      datahora_anterior,
      datahora_posterior,
      ponto_anterior,
      ponto_posterior,
      camera_anterior,
      camera_posterior,
      geolocation_anterior,
      geolocation_posterior,
      distancia_km,
      delta_horas,
      SAFE_DIVIDE(distancia_km, delta_horas) AS velocidade_implicita_kmh
    FROM pares_consecutivos
    WHERE
      distancia_km >= 1  -- Thresholhd distância mínima entre detecções
      AND SAFE_DIVIDE(distancia_km, delta_horas)
        >= 110  -- Thresholhd velocidade implícita mínima entre detecções
      AND DATE(datahora_anterior, 'America/Sao_Paulo')
        = DATE(datahora_posterior, 'America/Sao_Paulo')
  ),
  top_pares_suspeitos AS (
    SELECT placa, COUNT(*) AS pares_suspeitos
    FROM pares_suspeitos
    GROUP BY placa
    HAVING pares_suspeitos >= 500
  )
SELECT
  placa,
  'automática' AS tipo_insercao,
  CURRENT_DATE() AS data_insercao
FROM top_pares_suspeitos
