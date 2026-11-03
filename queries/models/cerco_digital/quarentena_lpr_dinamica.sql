{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['camera', 'origem_equipamento', 'data_dia'],
        partition_by={
            'field': 'data_dia',
            'data_type': 'date',
            'granularity': 'month',
        },
        cluster_by = ['data_dia', 'camera']
    )
}}

WITH leituras_validas AS (
  SELECT
    datahora,
    placa,
    id_ponto_coleta,
    camera_numero,
    camera_latitude AS latitude,
    camera_longitude AS longitude
  FROM {{ ref('vw_all_readings_pre_filter') }}
  WHERE 
    {% if is_incremental() %}
    datahora >= TIMESTAMP_ADD(TIMESTAMP((SELECT MAX(data_dia) FROM {{ this }}), 'America/Sao_Paulo'), INTERVAL 1 DAY)
    {% else %}
    datahora >= TIMESTAMP('2026-10-20 03:00:00')
    {% endif %}
    AND datahora < TIMESTAMP(CURRENT_DATE(), 'America/Sao_Paulo')
    AND REGEXP_CONTAINS(placa, r'^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$')
),

leituras_pares AS (
  SELECT
    placa,
    datahora AS datahora_posterior,
    id_ponto_coleta AS ponto_posterior,
    camera_numero AS camera_posterior,
    latitude AS latitude_posterior,
    longitude AS longitude_posterior,
    LAG(datahora) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS datahora_anterior,
    LAG(id_ponto_coleta) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS ponto_anterior,
    LAG(camera_numero) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS camera_anterior,
    LAG(latitude) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS latitude_anterior,
    LAG(longitude) OVER (
      PARTITION BY placa ORDER BY datahora, id_ponto_coleta, camera_numero
    ) AS longitude_anterior
  FROM leituras_validas
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
    ST_GEOGPOINT(longitude_anterior, latitude_anterior) AS geolocation_anterior,
    ST_GEOGPOINT(longitude_posterior, latitude_posterior) AS geolocation_posterior,
    SAFE_DIVIDE(
      ST_DISTANCE(
        ST_GEOGPOINT(longitude_anterior, latitude_anterior),
        ST_GEOGPOINT(longitude_posterior, latitude_posterior)
      ),
      1000.0
    ) AS distancia_km,
    SAFE_DIVIDE(TIMESTAMP_DIFF(datahora_posterior, datahora_anterior, SECOND), 3600.0) AS delta_horas
  FROM leituras_pares
  WHERE datahora_anterior IS NOT NULL
    AND TIMESTAMP_DIFF(datahora_posterior, datahora_anterior, SECOND) > 0
    AND ponto_anterior IS NOT NULL
    AND ponto_anterior != ponto_posterior
    AND camera_anterior != camera_posterior
),

pares_suspeitos AS (
SELECT
  CONCAT(placa, ponto_anterior, ponto_posterior, CAST(datahora_anterior AS STRING), CAST(datahora_posterior AS STRING)) AS id,
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
WHERE distancia_km >= 1  --Thresholhd distância mínima entre detecções
  AND SAFE_DIVIDE(distancia_km, delta_horas) >= 110  --Thresholhd velocidade implícita mínima entre detecções
  AND DATE(datahora_anterior, 'America/Sao_Paulo') = DATE(datahora_posterior, 'America/Sao_Paulo')
),

separados AS (
SELECT 
  data_dia, 
  ponto_anterior as ponto, 
  count(*) AS registros, 
  ARRAY_AGG(ponto_posterior) as pares_distintos,
  MAX(distancia_km) AS distancia_maxima,
  MAX(velocidade_implicita_kmh) AS velocidade_maxima
FROM pares_suspeitos
group by data_dia, ponto_anterior 

union all

SELECT 
  data_dia, 
  ponto_posterior as ponto, 
  count(*) AS registros, 
  ARRAY_AGG(ponto_anterior) as pares_distintos,
  MAX(distancia_km) AS distancia_maxima,
  MAX(velocidade_implicita_kmh) AS velocidade_maxima
FROM pares_suspeitos
group by data_dia, ponto_posterior
),

unificados AS (
SELECT 
  data_dia,
  ponto, 
  SUM(registros) AS registros,
  ARRAY_CONCAT_AGG(pares_distintos) pares,
  MAX(distancia_maxima) AS distancia_maxima,
  MAX(velocidade_maxima) AS velocidade_maxima
FROM separados GROUP BY data_dia, ponto
HAVING registros >= 20 AND
(distancia_maxima > 10 OR velocidade_maxima > 150)
ORDER BY registros DESC
),

pares_contagem AS (
SELECT 
  data_dia,
  ponto, 
  registros, 
  ARRAY(
    SELECT STRUCT(par, COUNT(*)/registros AS porcentagem)
    FROM UNNEST(pares) AS par
    GROUP BY par
    ) AS pares,
  ARRAY(
    SELECT STRUCT(par, COUNT(*)/registros AS porcentagem)
    FROM UNNEST(pares) AS par
    GROUP BY par
    HAVING COUNT(*)/registros > 0.2
    ) AS pares_superior_20_porcento
FROM unificados
),

unnested AS (
  SELECT data_dia, ponto, registros, par.par as par, par.porcentagem as porcentagem_par 
  FROM pares_contagem 
  LEFT JOIN UNNEST(pares) AS par
  WHERE ARRAY_LENGTH(pares_superior_20_porcento) > 0
),

nucleos_base AS (
SELECT 
  nucleo.data_dia,
  dependente.par as nucleo, 
  nucleo.registros as nucleo_registros, 
  dependente.ponto as dependente, 
  dependente.registros as dependente_registros
FROM unnested dependente
LEFT JOIN unnested nucleo
ON nucleo.ponto = dependente.par 
AND nucleo.par = dependente.ponto
AND dependente.data_dia = nucleo.data_dia
WHERE nucleo.registros >= dependente.registros AND
dependente.porcentagem_par > 0.5
),


nucleos AS (
  SELECT distinct data_dia, nucleo FROM nucleos_base where nucleo is not null
)

SELECT  
  c.data_dia,
  a.id_ponto_coleta,
  a.codigo_ponto_coleta,
  a.local,
  a.sentido,
  a.bairro,
  a.latitude,
  a.longitude,
  a.origem_equipamento,
  c.registros AS total_pares_suspeitos,
  c.pares AS pares_suspeitos
FROM {{ ref('ponto_coleta_lpr') }} a
JOIN nucleos b  
ON a.id_ponto_coleta = b.nucleo
LEFT JOIN pares_contagem c 
ON a.id_ponto_coleta = c.ponto AND b.data_dia = c.data_dia
WHERE ARRAY_LENGTH(c.pares) > 0
