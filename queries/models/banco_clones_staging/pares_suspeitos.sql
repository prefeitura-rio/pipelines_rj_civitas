{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        unique_key='id',
        partition_by={
            "field": "datahora_posterior",
            "data_type": "timestamp",
            "granularity": "month",
        },
        cluster_by=['placa'],
        incremental_predicates=[
            "DBT_INTERNAL_DEST.datahora_posterior > CURRENT_TIMESTAMP()" 
        ]
    )
}}

--Pegar coluna de controle incremental antes da execução
{% if is_incremental() %}
    {%- set max_datahora_query -%}
        SELECT MAX(datahora_posterior) FROM {{ this }}
    {%- endset -%}

    {%- set results = run_query(max_datahora_query) -%}

    {%- if execute and results and results.columns[0][0] is not none -%}
        {%- set max_datahora = results.columns[0][0] -%}
    {%- else -%}
        {%- set max_datahora = 'ERRO_INCREMENTAL_DADOS_INVALIDOS' -%}

    {%- endif -%}
{% endif %}


WITH leituras_validas AS (
  SELECT
    datahora,
    placa,
    id_ponto_coleta,
    camera_numero,
    camera_latitude AS latitude,
    camera_longitude AS longitude
  --FROM {{ ref('vw_all_readings') }} TODO 
  FROM `rj-civitas.cerco_digital.vw_all_readings`
  WHERE 
    {% if is_incremental() %}
      datahora > TIMESTAMP_SUB(TIMESTAMP('{{ max_datahora }}'), INTERVAL 2 HOUR) --Intervalo de 2 horas para pegar pares entre duas janelas de tempo da query
    {% else %}
      datahora >= TIMESTAMP('{{ var("start_date") }}', 'America/Sao_Paulo')
    {% endif %}
  
    AND REGEXP_CONTAINS(placa, r'^[A-Z]{3}[0-9][A-Z0-9][0-9]{2}$')
    AND id_ponto_coleta != '949' -- TODO: Tirar esses filtros manuais de câmera inválida
    AND camera_numero != '0530511121' -- TODO: Tirar esses filtros manuais de câmera inválida
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
  {% if is_incremental() %}
    AND datahora_posterior > TIMESTAMP('{{ max_datahora }}') --Filtro necessário para tirar pares duplicados, já que foi incluída janela de 2 horas para pares entre execuções
  {% endif %}
)

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
