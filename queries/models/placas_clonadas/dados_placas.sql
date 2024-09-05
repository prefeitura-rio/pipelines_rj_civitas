{{
  config(
    materialized = 'incremental',
    unique_key = 'placa',
    partition_by = {
        'field': 'datahora_inicial',
        'data_type': 'timestamp'
    },
    cluster_by = ['placa', 'camera_inicial', 'camera_final']
  )
}}

WITH movimentacao_placa AS (
    SELECT inicio.placa, 
        GREATEST(inicio.erro_datahora, fim.erro_datahora) AS erro_datahora,
        inicio.tipoveiculo AS tipo_inicial,
        inicio.velocidade AS velocidade_inicial,
        inicio.camera_numero AS camera_inicial,
        inicio.datahora AS datahora_inicial, 
        ST_GEOGPOINT(inicio.longitude, inicio.latitude) AS posicao_inicial,
        fim.tipoveiculo AS tipo_final,
        fim.velocidade AS velocidade_final,
        fim.camera_numero AS camera_final,
        fim.datahora AS datahora_final,
        ST_GEOGPOINT(fim.longitude, fim.latitude) AS posicao_final,
        (inicio.tipo_confiavel AND fim.tipo_confiavel) tipo_confiavel
    FROM `rj-civitas.placas_clonadas.dados_radares` inicio
    INNER JOIN `rj-civitas.placas_clonadas.dados_radares` fim
        ON inicio.placa = fim.placa
        AND inicio.datahora < fim.datahora
),
velocidade_veiculo AS (
  SELECT placa, GREATEST(150, MAX(velocidade) + 30) AS velocidade_maxima
  FROM `rj-civitas.placas_clonadas.dados_radares`
  GROUP BY placa
)

SELECT mp.placa,
    mp.camera_inicial,
    mp.camera_final,
    mp.posicao_inicial, 
    mp.posicao_final, 
    mp.datahora_inicial, 
    mp.datahora_final,
    mp.velocidade_inicial,
    mp.velocidade_final,
    (mp.tipo_inicial != mp.tipo_final) AS tipos_diferentes,
    mp.tipo_confiavel,
    ST_DISTANCE(mp.posicao_inicial, mp.posicao_final) AS distancia,
    3.6 * ST_DISTANCE(mp.posicao_inicial, mp.posicao_final) / (TIMESTAMP_DIFF(mp.datahora_final, mp.datahora_inicial, SECOND) + GREATEST(30, mp.erro_datahora)) AS velocidade_media,
    vv.velocidade_maxima
FROM movimentacao_placa mp
INNER JOIN velocidade_veiculo vv
    ON mp.placa = vv.placa
WHERE TIMESTAMP_DIFF(datahora_final, datahora_inicial, SECOND) < 300

-- Condição incremental
{% if is_incremental() %}
  AND mp.datahora_final > COALESCE(
    (SELECT MAX(datahora_final) FROM {{ this }}), 
    '2024-05-30'  -- Define uma data padrão antiga para a primeira execução
  )
{% endif %}
