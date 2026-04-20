{{
    config(
        materialized='table',
        cluster_by = ["id_ponto_coleta"]
    )
}}

WITH pontos AS (
  SELECT
    id_ponto_coleta,
    origem_equipamento AS origem_equipamento_lpr,
    codigo_ponto_coleta,
    local,
    bairro,
    sentido,
    latitude AS latitude_equip_lpr,
    longitude AS longitude_equip_lpr,
    ST_GEOGPOINT(SAFE_CAST(longitude AS FLOAT64), SAFE_CAST(latitude AS FLOAT64)) AS position
  FROM {{ ref('ponto_coleta_lpr') }}
),

cameras AS (
  SELECT
    codigo_camera AS id_camera,
    nome_camera,
    latitude,
    longitude,
    ST_GEOGPOINT(longitude, latitude) AS position,
    streaming_url,
    sistema_origem AS sistema_origem_camera
  FROM {{ ref('cameras') }}
),

ranked_cameras AS (
  SELECT
    t2.id_ponto_coleta,
    t2.origem_equipamento_lpr,
    t2.codigo_ponto_coleta,
    t2.sentido,
    t2.local,
    t2.bairro,
    t2.latitude_equip_lpr,
    t2.longitude_equip_lpr,
    t1.id_camera,
    t1.nome_camera,
    t1.streaming_url,
    t1.sistema_origem_camera,
    t1.latitude,
    t1.longitude,
    ST_DISTANCE(t1.position, t2.position) AS distance,
    ROW_NUMBER() OVER (PARTITION BY t2.id_ponto_coleta ORDER BY ST_DISTANCE(t1.position, t2.position)) AS rank
  FROM pontos t2
  LEFT JOIN  cameras t1
    ON ST_DWithin(t1.position, t2.position, 100000) -- Large enough distance to include all relevant cameras
  QUALIFY rank <= 5
)

  SELECT
    id_ponto_coleta,
    origem_equipamento_lpr,
    codigo_ponto_coleta,
    sentido,
    COALESCE(UPPER(local), 'Desconhecido') local,
    COALESCE(UPPER(bairro), 'Desconhecido') bairro,
    latitude_equip_lpr,
    longitude_equip_lpr,
    id_camera,
    nome_camera,
    sistema_origem_camera,
    streaming_url,
    latitude,
    longitude,
    distance,
    rank
  FROM ranked_cameras
  WHERE nome_camera is not null
