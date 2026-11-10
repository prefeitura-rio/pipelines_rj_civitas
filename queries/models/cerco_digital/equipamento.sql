{{
    config(
        materialized='table',
        partition_by={
            "field": "origem_equipamento",
            "data_type": "string"
        }
    )
}}

SELECT
    origem_equipamento,
    codigo_equipamento,
    codigo_ponto_coleta,
    TRIM(local) AS local,
    TRIM(sentido) AS sentido,
    TRIM(bairro) AS bairro,
    latitude,
    longitude,
    geography,
    status_ativo
 FROM {{ ref('stg_equipamento')}}
WHERE
    NOT ponto_coleta_ausente
    AND NOT codigo_equipamento_ausente
    AND NOT ponto_duplicado
    AND NOT ponto_com_latlongs_distantes
    AND NOT origem_equipamento_nao_reconhecida
    AND NOT sentido_nulo
    AND NOT bairro_nulo
    AND NOT local_nulo
    AND NOT latitude_errada
    AND NOT longitude_errada
    AND NOT status_nulo