{{
  config(
    materialized = "materialized_view",
    on_configuration_change = "apply",
    enable_refresh = True,
    refresh_interval_minutes = 1,
    max_staleness = "INTERVAL 0 MINUTE",
    partition_by={
        "field": "data_report",
        "data_type": "timestamp",
        "granularity": "month",
    },
    cluster_by = ["updated_at"]
    )
}}
SELECT
    CONCAT(id_source, id_report_original) AS id_report,
    *
FROM
    {{ source('stg_integracao_reports', 'reports_disque_denuncia') }}

UNION ALL

SELECT
    CONCAT(id_source, id_report_original) AS id_report,
    *
FROM
    {{ source('stg_integracao_reports', 'reports_1746') }}

UNION ALL

SELECT
  CONCAT(id_source, id_report_original) AS id_report,
  *
FROM
    {{ source('stg_integracao_reports', 'reports_fogo_cruzado') }}
UNION ALL

SELECT
  CONCAT(id_source, id_report_original) AS id_report,
  *
FROM
    {{ source('stg_integracao_reports', 'reports_telegram') }}

UNION ALL

SELECT
  CONCAT(id_source, id_report_original) AS id_report,
  *
FROM
    {{ source('stg_integracao_reports', 'reports_twitter') }}