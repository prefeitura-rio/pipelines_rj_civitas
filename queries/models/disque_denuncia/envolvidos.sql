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
    COALESCE(
        nome_envolvido,
        vulgo_envolvido,
        sexo_envolvido,
        CAST(idade_envolvido AS STRING),
        pele_envolvido,
        estatura_envolvido,
        olhos_envolvido,
        cabelos_envolvido,
        porte_envolvido,
        outras_caracteristicas_envolvido
    ) IS NOT NULL
