{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['codigo_equipamento', 'data_dia'],
        partition_by={
            'field': 'data_dia',
            'data_type': 'date',
            'granularity': 'month',
        },
        cluster_by = ['data_dia', 'codigo_equipamento']
    )
}}

{% if is_incremental() %}
    {%- set max_date_query -%}
        SELECT MAX(data_dia) FROM {{ this }}
    {%- endset -%}
    {%- set results = run_query(max_date_query) -%}

    {%- if execute and results and results.columns[0][0] is not none -%}
        {%- set max_date = results.columns[0][0] -%}
    {%- else -%}
        {%- set max_date = 'ERRO_INCREMENTAL_DADOS_INVALIDOS' -%}

    {%- endif -%}
{% endif %}


WITH leituras_validas AS (
  SELECT
    datahora,
    datahora_captura,
    placa,
    id_ponto_coleta,
    camera_numero,
    empresa,
    camera_latitude AS latitude,
    camera_longitude AS longitude
  FROM {{ ref('vw_all_readings_pre_filter') }}
  WHERE 
    {% if is_incremental() %}
    datahora >= TIMESTAMP_ADD(TIMESTAMP('{{ max_date }}', 'America/Sao_Paulo'), INTERVAL 1 DAY)
    {% else %}
    datahora >= TIMESTAMP('2026-11-01 03:00:00')
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

cameras_separadas AS (
SELECT 
  data_dia, 
  camera_anterior as camera, 
  ponto_anterior as ponto, 
  COUNT(*) AS registros
FROM pares_suspeitos
group by data_dia, camera_anterior, ponto_anterior

union all

SELECT 
  data_dia, 
  camera_posterior as camera,
  ponto_posterior as ponto, 
  COUNT(*) AS registros
FROM pares_suspeitos
group by data_dia, camera_posterior, ponto_posterior
),

cameras_unificadas AS (
  SELECT data_dia, camera, ponto, SUM(registros) AS pares_suspeitos
  FROM cameras_separadas
  GROUP BY ALL
),

pontos_separados AS (
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
FROM pontos_separados GROUP BY data_dia, ponto
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
  SELECT DISTINCT data_dia, nucleo FROM nucleos_base WHERE nucleo IS NOT NULL
),

cameras_nucleos AS (
  SELECT DISTINCT a.data_dia, a.camera, a.pares_suspeitos
  FROM cameras_unificadas a
  JOIN nucleos b
  ON a.ponto = b.nucleo AND a.data_dia = b.data_dia
  WHERE a.pares_suspeitos >= 20
),

cameras_clones_excessivos AS (
  SELECT  
  b.data_dia,
  a.codigo_equipamento,
  a.codigo_ponto_coleta,
  a.local,
  a.sentido,
  a.bairro,
  a.latitude,
  a.longitude,
  a.origem_equipamento,
  b.pares_suspeitos
FROM `rj-civitas.cerco_digital.equipamento` a
JOIN cameras_nucleos b  
ON a.codigo_equipamento = b.camera
),


latencia_base AS (
  SELECT
    PERCENTILE_CONT(TIMESTAMP_DIFF(datahora_captura, datahora, minute), 0.5)
      OVER (PARTITION BY COALESCE(b.empresa, a.empresa), date(datahora, 'America/Sao_Paulo'))
        latencia_mediana_empresa,
    datahora,
    datahora_captura,
    COALESCE(b.empresa, a.empresa) AS empresa,
    camera_numero,
    date(datahora, 'America/Sao_Paulo') AS data_dia
  FROM leituras_validas a
  LEFT JOIN `rj-civitas.cerco_digital.radar` b ON a.camera_numero = b.codcet AND a.empresa = 'CETRIO' 
),

latencia_agrupado AS (
  SELECT
    MIN(TIMESTAMP_DIFF(datahora_captura, datahora, minute))
      latencia_minima_camera,
    APPROX_QUANTILES(TIMESTAMP_DIFF(datahora_captura, datahora, minute), 100)[
      OFFSET(50)]
      latencia_mediana_camera,
    COUNT(*) AS deteccoes,
    latencia_mediana_empresa,
    camera_numero,
    empresa,
    data_dia
  FROM latencia_base
  GROUP BY ALL
),

cameras_latencia_excessiva AS (
SELECT
  deteccoes,
  data_dia,
  camera_numero AS codigo_equipamento,
  empresa,
  latencia_mediana_camera,
  latencia_mediana_empresa,
  latencia_mediana_camera - latencia_mediana_empresa AS diferenca_latencia,
  SUM(deteccoes) OVER (PARTITION BY data_dia, empresa) AS deteccoes_empresa_dia,
  SUM(deteccoes) OVER (PARTITION BY data_dia) AS deteccoes_dia
FROM latencia_agrupado
WHERE
  latencia_mediana_camera - latencia_mediana_empresa > 10
  OR latencia_mediana_camera < -5
),

uniao_razoes AS (
SELECT 
  data_dia,
  codigo_equipamento,
  CONCAT('Latência (', diferenca_latencia, ')') AS razao,
  diferenca_latencia,
  0 AS deteccoes_suspeitas_clonagem
FROM cameras_latencia_excessiva

UNION ALL

SELECT
  data_dia,
  codigo_equipamento,
  CONCAT('Pares suspeitos de clonagem (', pares_suspeitos,')') AS razao,
  0 AS diferenca_latencia,
  pares_suspeitos AS deteccoes_suspeitas_clonagem
FROM cameras_clones_excessivos
),

final AS (
SELECT 
  data_dia, 
  codigo_equipamento, 
  STRING_AGG(razao, ', ') AS razao,
  MAX(diferenca_latencia) AS diferenca_latencia,
  MAX(deteccoes_suspeitas_clonagem) AS deteccoes_suspeitas_clonagem
FROM uniao_razoes
GROUP BY data_dia, codigo_equipamento
)

SELECT a.*, COUNT(b.placa) AS leituras_dia
FROM final a
LEFT JOIN leituras_validas b
ON a.codigo_equipamento = b.camera_numero AND a.data_dia = DATE(b.datahora, 'America/Sao_Paulo')
GROUP BY ALL
