{{
  config(
    materialized = "materialized_view",
    on_configuration_change = "apply",
    enable_refresh = True,
    refresh_interval_minutes = 1,
    max_staleness = "INTERVAL 60 MINUTE",
    partition_by={
        "field": "data_report",
        "data_type": "datetime",
        "granularity": "month",
    }
    )
}}

SELECT
    CONCAT(id_source, id_report_original) AS id_report,
    *
FROM
    `integracao_reports_staging.reports_disque_denuncia`

UNION ALL

SELECT
    CONCAT(id_source, id_report_original) AS id_report,
    *
FROM
    `integracao_reports_staging.reports_1746`