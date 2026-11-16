WITH placa_monitorada AS (
    SELECT COUNT(*) AS quantidade
    FROM {{ ref('int_placa_monitorada__ativa') }}
),

caso_apoiado AS (
    SELECT COUNT(*) AS quantidade
    FROM {{ ref('int_caso_apoiado__unificado') }}
),

alerta_enviado AS (
    SELECT alerta_enviado_quantidade AS quantidade
    FROM {{ ref('stg_cerco_digital__alerta_enviado') }}
),

carga AS (
    SELECT MAX(carga_datahora) AS datahora
    FROM (
        SELECT carga_datahora FROM {{ ref('stg_app_civitas__ticket') }}
        UNION ALL
        SELECT carga_datahora FROM {{ ref('stg_app_civitas__placa_monitorada') }}
        UNION ALL
        SELECT carga_datahora FROM {{ ref('stg_app_civitas__placa_monitorada_vinculo') }}
    )
)

SELECT
    placa_monitorada.quantidade AS placa_monitorada_quantidade,
    caso_apoiado.quantidade AS caso_apoiado_quantidade,
    alerta_enviado.quantidade AS alerta_enviado_quantidade,
    carga.datahora AS carga_datahora
FROM placa_monitorada
CROSS JOIN caso_apoiado
CROSS JOIN alerta_enviado
CROSS JOIN carga
