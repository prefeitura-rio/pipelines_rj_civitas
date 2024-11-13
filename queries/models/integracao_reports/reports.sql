{{
  config(
    materialized = "materialized_view",
    on_configuration_change = "apply",
    enable_refresh = True,
    refresh_interval_minutes = 1,
    max_staleness = "INTERVAL 60 MINUTE",
    partition_by={
        "field": "data_report",
        "data_type": "timestamp",
        "granularity": "month",
    }
    )
}}
SELECT
    CONCAT(id_source, id_report_original) AS id_report,
    * EXCEPT (timestamp_insercao)
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
  * EXCEPT(timestamp_update)
FROM
    {{ source('stg_integracao_reports', 'reports_fogo_cruzado') }}
UNION ALL

SELECT
  CONCAT(id_source, id_report_original) AS id_report,
  * EXCEPT(timestamp_creation)
FROM
    {{ source('stg_integracao_reports', 'reports_telegram') }}