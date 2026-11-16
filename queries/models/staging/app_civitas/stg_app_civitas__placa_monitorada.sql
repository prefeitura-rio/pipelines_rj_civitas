{{
    config(
        alias = 'placa_monitorada'
    )
}}

WITH placa_monitorada AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'monitoredplate')) }}
)

-- A coluna legada `active` não é exposta: a API não a atualiza (ver documentação do source).
SELECT
    id AS id_placa_monitorada,
    UPPER(TRIM(plate)) AS placa,
    operation_id AS id_operacao,
    {{ codigo_para_texto('vehicle_type') }} AS veiculo_tipo,
    brand AS veiculo_marca,
    model AS veiculo_modelo,
    SAFE_CAST(model_year AS INT64) AS veiculo_modelo_ano,
    SAFE_CAST(manufacture_year AS INT64) AS veiculo_fabricacao_ano,
    color AS veiculo_cor,
    {{ codigo_para_texto('vehicle_info_source') }} AS veiculo_informacao_origem,
    notes AS observacao,
    contact_info AS contato,
    additional_info AS informacao_adicional,
    created_at AS criacao_datahora,
    updated_at AS origem_atualizacao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM placa_monitorada
