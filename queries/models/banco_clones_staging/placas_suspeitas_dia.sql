{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        unique_key='id',
        partition_by={
            "field": "data_dia",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=['placa'],
        incremental_predicates=[
            "DBT_INTERNAL_DEST.data_dia >= DATE_TRUNC(DATE_SUB(CURRENT_DATE('America/Sao_Paulo'), INTERVAL 1 MONTH), MONTH)" 
        ]
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


SELECT
  CONCAT(placa, CAST(data_dia AS STRING)) AS id,
  placa,
  data_dia,
  COUNT(*) AS pares_suspeitos,
  ROUND(MAX(distancia_km), 2) AS distancia_maxima,
  ROUND(MIN(distancia_km), 2) AS distancia_minima,
  ROUND(AVG(velocidade_implicita_kmh), 2) AS velocidade_implicita_media,
  ROUND(MAX(velocidade_implicita_kmh), 2) AS velocidade_implicita_maxima
  FROM {{ ref("pares_suspeitos") }}
  WHERE 
    {% if is_incremental() %}
      datahora_posterior >= TIMESTAMP('{{ max_date }}', 'America/Sao_Paulo')
    {% else %}
      datahora_posterior >= TIMESTAMP('{{ var("start_date") }}', 'America/Sao_Paulo')
    {% endif %}
  GROUP BY placa, data_dia
  HAVING COUNT(*) >= 4  --Thresholhd: número mínimo de detecções por dia
