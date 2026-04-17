{{
  config(
    materialized = 'table',
    primary_key = ['codcet']
    )
}}

WITH radar_data AS (
SELECT
    -- Remove todos os espaços em branco do código do ponto de coleta
    UPPER(TRIM(REGEXP_REPLACE(locequiP45, r'\s+', ''))) AS codigo_ponto_coleta,
    TRIM(LEFT(codcet, 3)) AS codigo_contrato,
    TRIM(codCET) AS codcet,
    UPPER(TRIM(REGEXP_REPLACE(locequip, r'\s+', ' '))) AS local,
    UPPER(TRIM(REGEXP_REPLACE(bairro, r'\s+', ' '))) AS bairro,
    SAFE_CAST(latitude AS FLOAT64) AS latitude,
    SAFE_CAST(longitude AS FLOAT64) AS longitude,
    UPPER(TRIM(REGEXP_REPLACE(logradouro, r'\s+', ' '))) AS logradouro,
    UPPER(TRIM(REGEXP_REPLACE(sentido, r'\s+', ' '))) AS sentido,
    SAFE_CAST(suspenso AS BOOLEAN) AS suspenso,
    UPPER(TRIM(status)) AS status,
    SAFE_CAST(inicioOperacao AS DATE) AS inicio_operacao,
    SAFE_CAST(fimContrato AS DATE) AS fim_contrato,
    UPPER(TRIM(REGEXP_REPLACE(empresa, r'\s+', ' '))) AS empresa
FROM 
    {{ source('stg_cerco_digital', 'radar') }}
)
SELECT 
  * 
FROM 
  radar_data 
WHERE 
  -- Excluindo contratos anteriores ao 048, pois não temos leituras destes equipamentos.
  SAFE_CAST(codigo_contrato AS INT64) >= 48