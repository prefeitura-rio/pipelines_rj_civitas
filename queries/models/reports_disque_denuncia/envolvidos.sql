{{
    config(
        materialized='table',
    )
}}

-- Select distinct records where at least one column is not null
SELECT DISTINCT
    id_envolvido,
    id_denuncia,
    nome_envolvido,
    vulgo_envolvido,
    sexo_envolvido,
    idade_envolvido,
    pele_envolvido,
    estatura_envolvido,
    olhos_envolvido,
    cabelos_envolvido,
    porte_envolvido,
    outras_caracteristicas_envolvido
FROM
    {{ref('stg_denuncias')}}
WHERE
    nome_envolvido IS NOT NULL
    OR vulgo_envolvido IS NOT NULL
    OR sexo_envolvido IS NOT NULL
    OR idade_envolvido IS NOT NULL
    OR pele_envolvido IS NOT NULL
    OR estatura_envolvido IS NOT NULL
    OR olhos_envolvido IS NOT NULL
    OR cabelos_envolvido IS NOT NULL
    OR porte_envolvido IS NOT NULL
    OR outras_caracteristicas_envolvido IS NOT NULL
