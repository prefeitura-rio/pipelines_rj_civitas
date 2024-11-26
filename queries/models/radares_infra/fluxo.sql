{{
  config(
    materialized = 'table',
    partition_by = {
        "field": "date",
        "data_type": "date",
        "granularity": "day"
    },
    cluster_by = ["date", "empresa", "camera_numero"]
    )
}}

SELECT
    camera_numero,
    empresa,
    DATE(datahora) AS date,
    COUNT(*) AS total_leituras,
	COUNTIF(velocidade = 0) AS total_velocidade_zero -- Contagem de leituras com velocidade = 0
FROM
    {{ source('ocr_radar', 'readings_2024') }}
WHERE
    datahora_captura >= datahora
GROUP BY
    empresa, camera_numero, DATE(datahora)