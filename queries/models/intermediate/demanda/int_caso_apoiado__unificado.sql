WITH caso AS (
    SELECT
        numero_referencia AS id_caso,
        entrada_data,
        'Controle de demandas' AS origem_tipo
    FROM {{ ref('stg_cerco_digital__controle_demanda') }}

    UNION ALL

    SELECT
        id_ticket AS id_caso,
        DATE(criacao_datahora, 'America/Sao_Paulo') AS entrada_data,
        'Ticket' AS origem_tipo
    FROM {{ ref('stg_app_civitas__ticket') }}
)

SELECT
    id_caso,
    MIN(entrada_data) AS entrada_data,
    ANY_VALUE(origem_tipo) AS origem_tipo
FROM caso
WHERE id_caso IS NOT NULL
    AND entrada_data >= DATE('2024-06-01')
GROUP BY id_caso
