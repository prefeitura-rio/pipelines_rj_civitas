{{
    config(
        alias = 'placa_monitorada_vinculo'
    )
}}

WITH vinculo AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'monitoredplateauthority')) }}
)

SELECT
    id AS id_placa_monitorada_vinculo,
    monitored_plate_id AS id_placa_monitorada,
    institution_authority_id AS id_autoridade_solicitante,
    reference_number AS numero_referencia,
    active AS ativo_indicador,
    monitor_all_collection_points AS todos_pontos_coleta_indicador,
    requested_at AS solicitacao_datahora,
    valid_until AS validade_datahora,
    created_at AS criacao_datahora,
    updated_at AS origem_atualizacao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM vinculo
