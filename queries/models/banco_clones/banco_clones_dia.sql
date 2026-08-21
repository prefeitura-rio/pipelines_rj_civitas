{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        unique_key='id',
        partition_by={
            "field": "dia",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by = ['placa'],
        incremental_predicates=[
            "DBT_INTERNAL_DEST.dia >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY), MONTH)" 
        ]
    )
}}


{% if is_incremental() %}
    {%- set max_date_query -%}
        SELECT MAX(dia) FROM {{ this }}
    {%- endset -%}
    {%- set results = run_query(max_date_query) -%}

    {%- if execute and results and results.columns[0][0] is not none -%}
        {%- set max_date = results.columns[0][0] -%}
    {%- else -%}
        {%- set max_date = 'ERRO_INCREMENTAL_DADOS_INVALIDOS' -%}

    {%- endif -%}
{% endif %}


WITH 
tokens_confundiveis AS (
  SELECT *
  FROM UNNEST([
    STRUCT('PMERJ' AS token, 70 AS peso),
    STRUCT('BOMBEIRO', 70),
    STRUCT('ESCOLA', 70),
    STRUCT('ESC', 60),
    STRUCT('POLICIA', 70),
    STRUCT('POL', 65),
    STRUCT('RIOITAG', 70), -- RIO1746 convertido para letras semelhantes
    STRUCT('ONIBUS', 65),
    STRUCT('ONI', 55),
    STRUCT('BRT', 45),
    STRUCT('TAXI', 45),
    STRUCT('GRU', 50),
    STRUCT('DIS', 45),
    STRUCT('CET', 40)
  ])
),

placas_isoladas AS (
    SELECT DISTINCT placa, data_dia 
    FROM {{ ref('placas_suspeitas_dia') }}
    WHERE 
    {% if is_incremental() %}
       data_dia > DATE('{{ max_date }}')
    {% else %}
      data_dia >= DATE('{{ var("start_date") }}')
    {% endif %}
    AND data_dia < CURRENT_DATE('America/Sao_Paulo')
    AND pares_suspeitos >=4 --Threshold mínimo pares suspeitos/dia
),

--SEPARAÇÃO DE SEQUÊNCIAS DE CARACTERES
chars_placas AS (
  SELECT
    placa,
    position,
    caractere
  FROM placas_isoladas,
  UNNEST(SPLIT(TRANSLATE(
                  placa,
                  '01234678',
                  'OIZEAGTB'
                ) , '')) AS caractere WITH OFFSET AS position
),
chars_placas_grupos AS (
  SELECT
    placa,
    caractere,
    COUNT(*) AS qtd
  FROM (
    SELECT
      placa,
      caractere,
      position,
      position - ROW_NUMBER() OVER (PARTITION BY placa, caractere ORDER BY position) AS grupo
    FROM chars_placas
  )
  GROUP BY placa, caractere, grupo
),
chars_placas_max_sequencia AS (
  SELECT
    placa,
    MAX(IF( caractere = 'O', qtd, 0)) AS max_sequencia_0o,
    MAX(IF( caractere = 'I', qtd, 0)) AS max_sequencia_1i,
    MAX(qtd) AS max_sequencia_geral
  FROM chars_placas_grupos
  GROUP BY placa
),

placas_suspeitas_pre_window_filter AS (
    SELECT 
        ps.placa,
        ps.data_dia,
        pi.data_dia as ultimo_dia_suspeito,
        DATE_SUB(pi.data_dia, INTERVAL 60 DAY) AS primeiro_dia_janela,
        ps.pares_suspeitos,
        ps.velocidade_implicita_maxima,
        ps.velocidade_implicita_media,
        ps.distancia_maxima,
        ps.distancia_minima
    FROM {{ ref('placas_suspeitas_dia') }} ps
    INNER JOIN placas_isoladas pi
    ON ps.placa = pi.placa
    WHERE 
    {% if is_incremental() %}
       ps.data_dia > DATE_SUB(DATE('{{ max_date }}'), INTERVAL 60 DAY)
    {% else %}
      ps.data_dia >= DATE_SUB(DATE('{{ var("start_date") }}'), INTERVAL 60 DAY)
    {% endif %}
    AND ps.data_dia < CURRENT_DATE('America/Sao_Paulo')
),

placas_suspeitas AS (
    SELECT 
        placa,
        ultimo_dia_suspeito,
        primeiro_dia_janela,
        SUM(pares_suspeitos) AS total_pares_suspeitos,
        COUNT(*) AS dias_com_suspeita,
        MAX(velocidade_implicita_maxima) AS velocidade_implicita_maxima,
        SUM(velocidade_implicita_media * pares_suspeitos) / SUM(pares_suspeitos) AS velocidade_implicita_media,
        MAX(distancia_maxima) AS distancia_maxima,
        MIN(distancia_minima) AS distancia_minima

    FROM placas_suspeitas_pre_window_filter
    WHERE data_dia >= primeiro_dia_janela
      AND data_dia <= ultimo_dia_suspeito
    GROUP BY placa, ultimo_dia_suspeito, primeiro_dia_janela
),

leituras_validas_pre_window_filter AS (
  SELECT
    DATE(datahora, 'America/Sao_Paulo') AS dia, 
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
      datahora >= TIMESTAMP_SUB(TIMESTAMP('{{ max_date }}', 'America/Sao_Paulo'), INTERVAL 60 DAY)
    {% else %}
      datahora >= TIMESTAMP_SUB(TIMESTAMP('{{ var("start_date") }}', 'America/Sao_Paulo'), INTERVAL 60 DAY)
    {% endif %}
    AND datahora < TIMESTAMP(CURRENT_DATE("America/Sao_Paulo"), "America/Sao_Paulo")
    AND placa IN (SELECT DISTINCT placa FROM placas_isoladas)
    AND id_ponto_coleta != '949' -- TODO: Tirar esses filtros manuais de câmera inválida
    AND camera_numero != '0530511121' -- TODO: Tirar esses filtros manuais de câmera inválida
),

leituras_validas AS (
  SELECT
    lv.dia,
    lv.datahora,
    lv.placa,
    ps.ultimo_dia_suspeito,
    lv.id_ponto_coleta,
    lv.camera_numero,
    lv.latitude,
    lv.longitude,
    COUNT(*) OVER (PARTITION BY lv.placa) AS total_leituras_placa
  FROM leituras_validas_pre_window_filter lv
  JOIN placas_suspeitas ps
  ON lv.placa = ps.placa
  WHERE lv.dia >= ps.primeiro_dia_janela
    AND lv.dia <= ps.ultimo_dia_suspeito
),

leituras_pares AS (
  SELECT
    placa,
    ultimo_dia_suspeito,
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

transicoes AS (
SELECT
    placa,
    ultimo_dia_suspeito,
    total_leituras_placa,
    SAFE_DIVIDE(
      ST_DISTANCE(ST_GEOGPOINT(longitude_a, latitude_a), ST_GEOGPOINT(longitude_b, latitude_b)),
      1000.0
    ) AS distancia_km
  FROM leituras_pares
  WHERE datahora_a IS NOT NULL
    AND ponto_a IS NOT NULL
    AND ponto_a != ponto_b
    AND TIMESTAMP_DIFF(datahora_b, datahora_a, SECOND) > 0
),

metricas_trajeto_e_placa AS (
SELECT
  placa,
  ultimo_dia_suspeito,
  TRANSLATE(
    placa,
    '01234678',
    'OIZEAGTB'
  ) AS placa_letras,
  total_leituras_placa,
  COUNT(*) AS transicoes,
  APPROX_QUANTILES(distancia_km, 100)[offset(50)] AS dist_mediana_km,
  APPROX_QUANTILES(distancia_km, 100)[offset(75)] AS dist_p75_km,
  APPROX_QUANTILES(distancia_km, 100)[offset(90)] AS dist_p90_km,
  SAFE_DIVIDE(COUNTIF(distancia_km > 10), COUNT(*)) AS pct_transicoes_maior_10km,
  SAFE_DIVIDE(COUNTIF(distancia_km < 1), COUNT(*)) AS pct_transicoes_menor_1km
FROM transicoes
GROUP BY placa, ultimo_dia_suspeito, placa_letras, total_leituras_placa
),
metricas_join AS (
SELECT
    mt.*,
    ps.total_pares_suspeitos,
    ps.dias_com_suspeita,
    ps.velocidade_implicita_maxima,
    ps.velocidade_implicita_media,
    ps.distancia_maxima,
    ps.distancia_minima,
    ( SELECT MAX(qtd)
      FROM (
        SELECT
          caractere,
          COUNT(*) AS qtd
        FROM UNNEST(REGEXP_EXTRACT_ALL(RIGHT(mt.placa_letras, 4), r'.')) caractere
        GROUP BY caractere
      )
  ) AS maior_repeticao_ultimos_4_caracteres,
  ( SELECT - ROUND( SUM( (qtd / 7) * LOG(qtd / 7, 2) ), 3) 
      FROM (
        SELECT
          caractere,
          COUNT(*) AS qtd
        FROM UNNEST(REGEXP_EXTRACT_ALL(mt.placa, r'.')) caractere
        GROUP BY caractere
      )
  ) AS entropia,
  LENGTH(mt.placa) - LENGTH(REPLACE(mt.placa_letras, 'O', '')) AS qtd_possiveis_0o,
  LENGTH(mt.placa) - LENGTH(REPLACE(mt.placa_letras, 'I', '')) AS qtd_possiveis_1i,
  COALESCE((
      SELECT MAX(tc.peso) 
      FROM tokens_confundiveis tc
      WHERE STRPOS(mt.placa_letras, tc.token) > 0
    ), 0) AS peso_token_confundivel
FROM metricas_trajeto_e_placa mt 
JOIN placas_suspeitas ps
ON mt.placa = ps.placa AND mt.ultimo_dia_suspeito = ps.ultimo_dia_suspeito
),

placas_score_temperatura AS (
SELECT
    mj.placa,
    CURRENT_TIMESTAMP() AS timestamp_insercao,
    mj.ultimo_dia_suspeito,

    /*
    SCORE TRAJETO: avalia o quanto o trajeto geral do veículo parece real. 
      -Quanto menor o score, melhor a qualidade do trajeto.
      -Um score alto indica que provavelmente são leituras erradas 
      de alguma imagem que não a placa de um veículo, como adesivos e banners. 
    */
    LEAST(100,
    CASE 
        WHEN mj.dist_mediana_km >= 10 THEN 30
        WHEN mj.dist_mediana_km >= 5 THEN 18
        ELSE 0
        END
        +
    CASE
        WHEN mj.dist_p75_km >= 15 THEN 20
        WHEN mj.dist_p75_km >= 10 THEN 12
        ELSE 0
        END
        +
    CASE
        WHEN mj.dist_p90_km >= 30 THEN 18
        WHEN mj.dist_p90_km >= 20 THEN 10
        ELSE 0
        END
        +
    CASE
        WHEN mj.pct_transicoes_maior_10km >= 0.5 THEN 18
        WHEN mj.pct_transicoes_maior_10km >= 0.25 THEN 10
        ELSE 0
        END
        +
    CASE
        WHEN mj.pct_transicoes_menor_1km <= 0.05 THEN 12
        WHEN mj.pct_transicoes_menor_1km <= 0.15 THEN 6
        ELSE 0
        END
        +
    CASE
        WHEN mj.transicoes >= 1000 AND mj.dist_mediana_km >= 2 THEN 10
        ELSE 0
        END
      )  AS score_trajeto,
    
    /*
    SCORE OCR: avalia o quanto a placa do veículo parece real. 
      -Quanto menor o score, mais provável de ser uma placa real.
      -Um score alto indica que provavelmente são leituras erradas 
      de alguma imagem que não a placa de um veículo, como adesivos e banners. 
    */
    LEAST(100,
    mj.peso_token_confundivel
      +
    CASE
      WHEN mj.maior_repeticao_ultimos_4_caracteres = 4 THEN 25
      WHEN mj.maior_repeticao_ultimos_4_caracteres = 3 THEN 18
      WHEN mj.maior_repeticao_ultimos_4_caracteres = 2 THEN 8
      ELSE 0
      END
      +
    CASE
      WHEN mj.entropia < 1.8 THEN 35
      WHEN mj.entropia < 2.1 THEN 20
      WHEN mj.entropia < 2.3 THEN 10
      ELSE 0
      END
      +
    CASE
      WHEN mj.qtd_possiveis_0o >= 4 THEN 25
      WHEN mj.qtd_possiveis_0o = 3 THEN 18
      WHEN mj.qtd_possiveis_0o = 2 THEN 8
      ELSE 0
      END
      +
    CASE
      WHEN mj.total_pares_suspeitos >= 50 THEN 20
      WHEN mj.total_pares_suspeitos >= 20 THEN 10
      ELSE 0
      END
      +
    CASE
      WHEN mj.total_leituras_placa >= 5000 THEN 15
      WHEN mj.total_leituras_placa >= 2000 THEN 8
      ELSE 0
      END
      +
    CASE
      WHEN mj.dias_com_suspeita >= 3 THEN 6
      ELSE 0
      END
      +
    CASE
      WHEN mj.velocidade_implicita_maxima >= 5000
      THEN 8
      ELSE 0
      END
      +
    CASE
      WHEN cpms.max_sequencia_geral >= 4 THEN 20
      WHEN cpms.max_sequencia_geral = 3 THEN 10
      ELSE 0
      END
      +
    LEAST(60,
        CASE
          WHEN mj.qtd_possiveis_0o + mj.qtd_possiveis_1i >= 4 THEN 22
          WHEN mj.qtd_possiveis_0o + mj.qtd_possiveis_1i = 3 THEN 12
          ELSE 0
          END 
          +
        IF(CONTAINS_SUBSTR(mj.placa, '0') AND CONTAINS_SUBSTR(mj.placa, 'O'), 10, 0)
          +
        IF(CONTAINS_SUBSTR(mj.placa, '1') AND CONTAINS_SUBSTR(mj.placa, 'I'), 10, 0)
          +
        CASE
          WHEN GREATEST(cpms.max_sequencia_0o, cpms.max_sequencia_1i) >= 4 THEN 18
          WHEN GREATEST(cpms.max_sequencia_0o, cpms.max_sequencia_1i) = 3 THEN 10
          ELSE 0
          END
          +
        CASE
          WHEN mj.qtd_possiveis_0o + mj.qtd_possiveis_1i >= 3 AND mj.total_pares_suspeitos >=50 THEN 12
          WHEN mj.qtd_possiveis_0o + mj.qtd_possiveis_1i >= 3 AND mj.total_pares_suspeitos >=20 THEN 6
          ELSE 0
          END
          )
     ) AS score_ocr,

    LEAST(100,
    CASE
      WHEN mj.total_pares_suspeitos >= 10 THEN 15
      WHEN mj.total_pares_suspeitos >= 7 THEN 10
      WHEN mj.total_pares_suspeitos >= 5 THEN 5
      ELSE 0
      END
      +
    CASE
      WHEN mj.velocidade_implicita_media >= 1500 THEN 30
      WHEN mj.velocidade_implicita_media >= 800 THEN 25
      WHEN mj.velocidade_implicita_media >= 300 THEN 18
      WHEN mj.velocidade_implicita_media >= 150 THEN 8
      ELSE 0
      END
      +
    CASE
      WHEN mj.velocidade_implicita_maxima >= 3000 THEN 20
      WHEN mj.velocidade_implicita_maxima >= 1500 THEN 16
      WHEN mj.velocidade_implicita_maxima >= 500 THEN 10
      WHEN mj.velocidade_implicita_maxima >= 180 THEN 4
      ELSE 0
      END
      +
    CASE
      WHEN mj.distancia_maxima >= 30 THEN 20
      WHEN mj.distancia_maxima >= 15 THEN 15
      WHEN mj.distancia_maxima >= 8 THEN 8
      ELSE 0
      END
      +
    CASE
      WHEN mj.distancia_minima >= 15 THEN 20
      WHEN mj.distancia_minima >= 10 THEN 10
      WHEN mj.distancia_minima >= 5 THEN 5
      ELSE 0
      END
    ) AS temperatura
    
FROM metricas_join mj
LEFT JOIN chars_placas_max_sequencia cpms
ON cpms.placa = mj.placa
)

SELECT
  CONCAT(placa, CAST(ultimo_dia_suspeito AS STRING)) AS id,
  placa,
  ultimo_dia_suspeito AS dia,
  score_ocr,
  score_trajeto,
  temperatura,
  timestamp_insercao
FROM placas_score_temperatura
WHERE score_trajeto < 90
  AND (score_ocr < 60 OR score_trajeto <= 10)