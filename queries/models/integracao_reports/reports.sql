{{
  config(
    materialized = 'table',
    unique_key='id_report',
    partition_by={
        "field": "data_report",
        "data_type": "datetime",
        "granularity": "hour",
    }
    )
}}

SELECT
    CONCAT(id_source, id_report_original) AS id_report,
    *
FROM
    {{ ref("stg_disque_denuncia_reports") }}

UNION ALL

SELECT
    CONCAT(id_source, id_report_original) AS id_report,
    *
FROM
    {{ ref("stg_1746_reports") }}