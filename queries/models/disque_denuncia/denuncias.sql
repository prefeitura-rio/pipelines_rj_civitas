{{
    config(
        materialized='table',
        unique_key='id_denuncia',
        partition_by={
            "field": "data_denuncia",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}
-- CTE to rank records within each id_denuncia partition
WITH denuncias_ranked AS (
    SELECT
        *,
        -- Assign a row number within each id_denuncia partition, ordered by nome_arquivo (descending) and data_difusao (descending)
        ROW_NUMBER() OVER (PARTITION BY id_denuncia ORDER BY nome_arquivo DESC, data_difusao DESC) AS ranking_unique
    FROM
        {{ref('stg_denuncias')}}
    WHERE
        ranking = 1
)

-- Final selection of the top-ranked records (ranking = 1) for each id_denuncia
SELECT
    id_denuncia,
    numero_denuncia,
    data_denuncia,
    data_difusao,
    tipo_logradouro,
    logradouro,
    numero_logradouro,
    complemento_logradouro,
    bairro,
    subbairro,
    cep_logradouro,
    referencia_logradouro,
    municipio,
    estado,
    latitude,
    longitude,
    relato
FROM
    denuncias_ranked
WHERE
    ranking_unique = 1
