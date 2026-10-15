{{
    config(
        materialized='ephemeral'
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
      COALESCE(b.nome, c.nome_bairro) AS bairro,
      a.latitude,
      a.longitude,
      ST_GEOGPOINT(SAFE_CAST(a.longitude AS FLOAT64), SAFE_CAST(a.latitude AS FLOAT64)) AS geography,
      status AS status_ativo
    FROM {{ ref('lpr_cameras') }} a
    LEFT JOIN {{ source('datario', 'bairro') }} b ON ST_WITHIN(ST_GEOGPOINT(a.longitude, a.latitude), b.geometry)
    LEFT JOIN {{ source('dados_geograficos', 'bairros_rj')}} c ON ST_WITHIN(ST_GEOGPOINT(a.longitude, a.latitude), c.geometria)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY codigo_equipamento ORDER BY bairro) = 1
),
gabriel_lpr AS (
    SELECT
      'GABRIEL' AS origem_equipamento,
      a.camera_numero AS codigo_equipamento,
      --ponto de coleta é o latlong, pegando apenas os pares de câmeras em cada endereço como uma só
      CONCAT(TRUNC(camera_latitude, 6), TRUNC(camera_longitude, 6)) AS codigo_ponto_coleta,
      a.camera_logradouro AS local,
      'UNICO' AS sentido,
      COALESCE(b.nome, c.nome_bairro) AS bairro,
      a.camera_latitude AS latitude,
      a.camera_longitude AS longitude,
      ST_GEOGPOINT(SAFE_CAST(a.camera_longitude AS FLOAT64), SAFE_CAST(a.camera_latitude AS FLOAT64)) AS geography,
      TRUE AS status_ativo
    FROM {{ source('brutos_gabriel', 'devices') }} a
    LEFT JOIN {{ source('datario', 'bairro') }} b ON ST_WITHIN(ST_GEOGPOINT(a.camera_longitude, a.camera_latitude), b.geometry)
    LEFT JOIN {{ source('dados_geograficos', 'bairros_rj')}} c ON ST_WITHIN(ST_GEOGPOINT(a.longitude, a.latitude), c.geometria)
    QUALIFY
    (
      ROW_NUMBER() OVER (
        PARTITION BY codigo_equipamento
        ORDER BY created_at DESC
      )
    ) = 1
),
all_equipments AS (
    SELECT * FROM cetrio_radars

    UNION ALL

    SELECT * FROM civitas_lpr

    UNION ALL

    SELECT * FROM gabriel_lpr
),
all_equipments_cleaned AS (
    SELECT
        origem_equipamento,
        codigo_equipamento,
        -- Remove todos os espaços em branco do código do ponto de coleta
        TRIM(REGEXP_REPLACE(codigo_ponto_coleta, r'\s+', '')) AS codigo_ponto_coleta,
        -- Remove espaços extras (mais de um espaço) e substitui por um espaço simples
        REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(local)), NFD), r'\pM', ''), r'\s+', ' ') AS local,
        REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(sentido)), NFD), r'\pM', ''), r'\s+', ' ') AS sentido,
        REGEXP_REPLACE(REGEXP_REPLACE(NORMALIZE(UPPER(TRIM(bairro)), NFD), r'\pM', ''), r'\s+', ' ') AS bairro,
        latitude,
        longitude,
        geography,
        status_ativo,
        COUNT(DISTINCT CONCAT(sentido, bairro, local)) OVER(PARTITION BY codigo_ponto_coleta) repeticoes_codigo_ponto_coleta
    FROM all_equipments
),
latlong_distante AS (
    SELECT DISTINCT
      a.codigo_ponto_coleta,
      a.origem_equipamento
    FROM all_equipments_cleaned a
    LEFT JOIN all_equipments_cleaned b
    ON a.codigo_ponto_coleta = b.codigo_ponto_coleta 
    AND a.origem_equipamento = b.origem_equipamento
    AND ST_DISTANCE(a.geography, b.geography) > 1000
    WHERE b.codigo_ponto_coleta IS NOT NULL
)

SELECT
    a.*,
    COALESCE(a.codigo_ponto_coleta, '') = '' AS ponto_coleta_ausente,
    COALESCE(a.codigo_equipamento, '') = '' AS codigo_equipamento_ausente,
    a.repeticoes_codigo_ponto_coleta > 1 AS ponto_duplicado,
    b.codigo_ponto_coleta IS NOT NULL AS ponto_com_latlongs_distantes,
    COALESCE(a.origem_equipamento, '') NOT IN ('CETRIO', 'CIVITAS') AS origem_equipamento_nao_reconhecida,
    a.sentido IS NULL AS sentido_nulo,
    a.bairro IS NULL AS bairro_nulo,
    a.local IS NULL AS local_nulo,
    COALESCE(a.latitude, 1) NOT BETWEEN -90 AND 0 AS latitude_errada,
    COALESCE(a.longitude, 1) NOT BETWEEN -90 AND 0 AS longitude_errada,
    a.status_ativo IS NULL AS status_nulo
    
FROM all_equipments_cleaned a
LEFT JOIN latlong_distante b
ON a.codigo_ponto_coleta = b.codigo_ponto_coleta AND a.origem_equipamento = b.origem_equipamento

