{{
    config(
        materialized='incremental',
        partition_by={
            "field": "datetime_ultima_atualizacao",
            "data_type": "datetime",
            "granularity": "day",
        }
    )
}}
-- CTE base
WITH base_query AS (
  SELECT
    modo,
    ano_fabricacao,
    carroceria,
    nome_chassi,
    placa,
    tipo_combustivel,
    tipo_veiculo,
    datetime_ultima_atualizacao,
    ROW_NUMBER() OVER(PARTITION BY placa ORDER BY datetime_ultima_atualizacao DESC) rn
  FROM
    {{ source('smtr_veiculo', 'licenciamento') }}

  {% if is_incremental() %}
  WHERE
    datetime_ultima_atualizacao > (SELECT MAX(datetime_ultima_atualizacao) FROM {{ this }})
    OR placa NOT IN (SELECT DISTINCT placa FROM {{ this }})
  {% endif %}
  QUALIFY rn = 1
)
SELECT
  * EXCEPT(rn)
FROM
  base_query