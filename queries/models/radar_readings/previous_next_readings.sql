-- dbt incremental config
{{ config(
    materialized='incremental', -- Usar incremental para garantir que as leituras sejam atualizadas corretamente
    unique_key='camera_numero_datahora', -- Definir uma chave única para garantir que as leituras sejam atualizadas corretamente
    incremental_strategy='merge', -- Usar a estratégia de merge ao invés de insert_overwrite
    partition_by={
      "field": "datahora", -- Particiona por datahora (TIMESTAMP)
      "data_type": "timestamp" -- Define que é um campo do tipo timestamp
    },
    cluster_by=['camera_numero'] -- Cria clusters pelos valores de camera_numero dentro de cada partição
) }}


WITH leituras_por_camera AS (
  -- Cálculo da próxima leitura futura e última leitura anterior para cada câmera
  SELECT
    camera_numero,
    datahora,
    DATE(datahora) AS dia, -- Extrair apenas a data
    LEAD(datahora) OVER (PARTITION BY camera_numero ORDER BY datahora ASC) AS proxima_leitura_futura, -- Próxima leitura futura
    LAG(datahora) OVER (PARTITION BY camera_numero ORDER BY datahora ASC) AS ultima_leitura_anterior -- Última leitura anterior
  FROM `rj-cetrio.ocr_radar.readings_2024*`
  {% if is_incremental() %}
    WHERE datahora >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  {% endif %}
),

-- Gerar todas as combinações de câmeras e dias, considerando o intervalo de datas correto
cameras_e_datas AS (
  SELECT 
    DISTINCT lc.camera_numero, -- Mantém todas as câmeras
    td AS dia -- Junta todas as datas
  FROM (SELECT DISTINCT camera_numero FROM leituras_por_camera) lc
  CROSS JOIN UNNEST(
    {% if is_incremental() %}
      -- Executa apenas para os últimos 7 dias a partir do último timestamp processado até o dia atual
      GENERATE_DATE_ARRAY(DATE_SUB((SELECT MAX(datahora) FROM {{ this }}), INTERVAL 7 DAY), CURRENT_DATE())
    {% else %}
      -- Na primeira execução, processa desde o início até a data atual
      GENERATE_DATE_ARRAY('2024-05-30', CURRENT_DATE())
    {% endif %}
  ) AS td
),

-- Unir as leituras reais com todas as datas
uniao_leituras AS (
  SELECT 
    cd.camera_numero,
    cd.dia,
    lc.datahora,
    lc.proxima_leitura_futura,
    lc.ultima_leitura_anterior
  FROM cameras_e_datas cd
  LEFT JOIN leituras_por_camera lc
    ON cd.camera_numero = lc.camera_numero
    AND cd.dia = DATE(lc.datahora)
),

-- Propagar a próxima leitura futura e última leitura anterior corretamente
leituras_propagadas AS (
  SELECT
    camera_numero,
    COALESCE(datahora, TIMESTAMP(dia)) AS datahora, -- Substitui datahora nulo pela data como timestamp
    -- Propagação da última leitura válida
    COALESCE(
      ultima_leitura_anterior,
      LAG(datahora) OVER (PARTITION BY camera_numero ORDER BY dia ASC),
      MAX(datahora) OVER (PARTITION BY camera_numero ORDER BY dia ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS ultima_leitura_valida,
    -- Propagação da próxima leitura futura
    COALESCE(proxima_leitura_futura,
      MAX(proxima_leitura_futura) OVER (PARTITION BY camera_numero ORDER BY dia ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS proxima_leitura_valida
  FROM uniao_leituras
)

-- Mesclar os dados novos com os existentes
SELECT
  COALESCE(novos.camera_numero, antigos.camera_numero) AS camera_numero,
  COALESCE(novos.datahora, antigos.datahora) AS datahora, -- Caso novos dados tenham nulo, usa os antigos
  COALESCE(novos.ultima_leitura_valida, antigos.ultima_leitura_valida) AS ultima_leitura_ou_anterior, -- Mescla os valores
  COALESCE(novos.proxima_leitura_valida, antigos.proxima_leitura_valida) AS proxima_leitura_futura, -- Mescla os valores
  CONCAT(COALESCE(novos.camera_numero, antigos.camera_numero), "_", CAST(COALESCE(novos.datahora, antigos.datahora) AS STRING)) AS camera_numero_datahora -- Chave única mesclada
FROM (
  -- Dados incrementais (novos)
  SELECT *
  FROM leituras_propagadas
) AS novos
FULL OUTER JOIN {{ this }} AS antigos -- Mescla com os dados antigos já existentes
ON novos.camera_numero = antigos.camera_numero
AND novos.datahora = antigos.datahora
