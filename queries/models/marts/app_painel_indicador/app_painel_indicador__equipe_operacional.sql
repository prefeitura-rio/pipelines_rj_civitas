{{
    config(
        alias = 'equipe_operacional'
    )
}}

SELECT
    id_equipe,
    nome AS equipe_nome,
    exibicao_ordem
FROM {{ ref('int_equipe__operacional') }}
