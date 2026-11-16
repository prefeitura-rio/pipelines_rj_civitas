WITH vinculo_ativo AS (
    SELECT
        id_placa_monitorada,
        COUNT(*) AS vinculo_ativo_quantidade,
        MIN(validade_datahora) AS validade_proxima_datahora
    FROM {{ ref('stg_app_civitas__placa_monitorada_vinculo') }}
    WHERE ativo_indicador
    GROUP BY id_placa_monitorada
)

SELECT
    placa.id_placa_monitorada,
    placa.placa,
    vinculo_ativo.vinculo_ativo_quantidade,
    vinculo_ativo.validade_proxima_datahora
FROM {{ ref('stg_app_civitas__placa_monitorada') }} AS placa
INNER JOIN vinculo_ativo
    ON vinculo_ativo.id_placa_monitorada = placa.id_placa_monitorada
