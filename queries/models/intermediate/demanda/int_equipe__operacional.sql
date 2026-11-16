WITH ordem AS (
    SELECT equipe_nome, exibicao_ordem
    FROM UNNEST({{ var('painel_equipes_operacionais') }}) AS equipe_nome WITH OFFSET AS exibicao_ordem
)

SELECT
    equipe.id_equipe,
    equipe.nome,
    ordem.exibicao_ordem
FROM {{ ref('stg_app_civitas__equipe') }} AS equipe
INNER JOIN ordem
    ON ordem.equipe_nome = LOWER(equipe.nome)
WHERE equipe.ativo_indicador IS TRUE
