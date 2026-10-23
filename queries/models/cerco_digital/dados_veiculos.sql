{{
    config(
        tags = ['hourly'],
        materialized = 'incremental',
        incremental_strategy='merge',
        unique_key=['placa'],
        cluster_by=['placa'],
        merge_exclude_columns=['datahora_primeira_insercao']
    )
}}

{% if is_incremental() %}
WITH max_datahora AS (
    SELECT MAX(max_datahora_lote) as ultima_datahora FROM {{ this }}
)
{% endif %}

SELECT 
    placa,
    tipo_veiculo_prf,
    cor_veiculo_prf,
    marca_modelo_prf,
    ano_modelo_prf,
    ano_fabricacao_prf,
    CURRENT_TIMESTAMP() AS datahora_primeira_insercao,
    datahora AS datahora_ultima_deteccao,
    MAX(datahora) OVER() AS max_datahora_lote
FROM {{ source('lpr', 'readings_raw') }}
WHERE 
COALESCE(tipo_veiculo_prf, '') != '' AND
{% if is_incremental() %}
datahora > (SELECT ultima_datahora FROM max_datahora)
{% else %}
datahora >= '2026-08-01 03:00:00'
{% endif %}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY placa
    ORDER BY datahora DESC
) = 1