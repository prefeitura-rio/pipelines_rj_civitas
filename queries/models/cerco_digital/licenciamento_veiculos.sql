{{
    config(
        materialized='table',
        partition_by={
            "field": "datetime_ultima_atualizacao",
            "data_type": "datetime",
            "granularity": "day",
        },
        unique_key = ['placa']
    )
}}
WITH all_data AS (
  SELECT
    modo,
    ano_fabricacao,
    carroceria,
    nome_chassi,
    placa,
    tipo_combustivel,
    tipo_veiculo,
    datetime_ultima_atualizacao
  FROM {{ source('smtr_veiculo_old', 'licenciamento') }}

  UNION ALL

  SELECT
    modo,
    ano_fabricacao,
    carroceria,
    nome_chassi,
    placa,
    tipo_combustivel,
    tipo_veiculo,
    datetime_ultima_atualizacao
  FROM {{ source('smtr_veiculo', 'veiculo_licenciamento_dia') }}
)
SELECT
  modo,
  ano_fabricacao,
  carroceria,
  nome_chassi,
  placa,
  tipo_combustivel,
  tipo_veiculo,
  datetime_ultima_atualizacao
FROM all_data
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY placa
  ORDER BY datetime_ultima_atualizacao DESC
) = 1
