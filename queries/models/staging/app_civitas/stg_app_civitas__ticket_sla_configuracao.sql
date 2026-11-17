{{
    config(
        alias = 'ticket_sla_configuracao'
    )
}}

WITH ticket_sla_configuracao AS (
    {{ dedup_airbyte_cdc(source('app_civitas', 'ticket_dashboard_sla_configs')) }}
)

SELECT
    id AS id_configuracao,
    electronic_fence_days AS cerco_eletronico_sla_dias,
    license_plate_search_days AS busca_placa_sla_dias,
    radar_search_days AS busca_radar_sla_dias,
    related_license_plates_days AS placa_correlata_sla_dias,
    joint_license_plates_days AS placa_conjunta_sla_dias,
    image_search_days AS busca_imagem_sla_dias,
    image_analysis_days AS analise_imagem_sla_dias,
    atlas_civitas_days AS atlas_civitas_sla_dias,
    others_days AS outro_sla_dias,
    created_at AS criacao_datahora,
    updated_at AS origem_atualizacao_datahora,
    _airbyte_extracted_at AS carga_datahora
FROM ticket_sla_configuracao
