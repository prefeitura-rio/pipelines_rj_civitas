{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'id_report_original',
    partition_by={
        "field": "data_report",
        "data_type": "timestamp",
        "granularity": "month",
    },
    cluster_by = ["timestamp_update"]
    )
}}

WITH ocorrencias AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id_ocorrencia ORDER BY timestamp_update DESC) AS rn
  FROM
    {{ source('fogo_cruzado', 'ocorrencias') }}

  {% if is_incremental() %}
  WHERE
    timestamp_update > (SELECT max(timestamp_update) FROM {{ this }})
  {% endif %}
  QUALIFY rn = 1
),
orgaos_agg AS (
  SELECT
    id_ocorrencia AS id_report_original,
    ARRAY_AGG('Fogo Cruzado') AS orgaos
  FROM
    ocorrencias
  GROUP BY
    id_report_original
),
subtipo_agg AS (
  SELECT
    id_ocorrencia AS id_report_original,
    ARRAY_AGG(
      STRUCT(
        LOWER(motivo_principal) AS tipo,
        ARRAY(
          SELECT LOWER(elem)
          FROM UNNEST(ARRAY_CONCAT(motivos_complementares, categorias)) AS elem
        ) AS subtipo
      )
    ) AS tipo_subtipo
  FROM
    ocorrencias
  GROUP BY
    id_ocorrencia
)
  SELECT
    'FC' AS id_source,
    id_ocorrencia AS id_report_original,
    TIMESTAMP(data_ocorrencia, 'America/Sao_Paulo') AS data_report,
    o.orgaos,
    'Tiroteio' AS categoria,
    t.tipo_subtipo,
    '' AS descricao,
    TRIM(REGEXP_EXTRACT(endereco, r'^(.*?)(?:-|,)')) AS logradouro,
    '' AS numero_logradouro,
    latitude,
    longitude,
    timestamp_update
  FROM ocorrencias c
  LEFT JOIN orgaos_agg o ON c.id_ocorrencia = o.id_report_original
  LEFT JOIN subtipo_agg t ON c.id_ocorrencia = t.id_report_original
