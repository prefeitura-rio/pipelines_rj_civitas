{{
    config(
        alias = 'controle_demanda'
    )
}}

WITH controle_demanda AS (
    SELECT 2024 AS planilha_ano, *
    FROM {{ source('cerco_digital_planilha', 'controle_demandas_operacional_2024') }}

    UNION ALL

    SELECT 2025 AS planilha_ano, *
    FROM {{ source('cerco_digital_planilha', 'controle_demandas_operacional_2025') }}

    UNION ALL

    SELECT 2026 AS planilha_ano, *
    FROM {{ source('cerco_digital_planilha', 'controle_demandas_operacional_2026') }}
),

tratado AS (
    SELECT
        NULLIF(UPPER(TRIM(numero_referencia)), '') AS numero_referencia,
        NULLIF(TRIM(numero_oficio), '') AS numero_oficio,
        NULLIF(TRIM(servicos), '') AS servicos,
        NULLIF(TRIM(requisitante), '') AS solicitante_nome,
        NULLIF(LOWER(TRIM(email_requisitante)), '') AS solicitante_email,
        NULLIF(TRIM(oficio_resposta), '') AS oficio_resposta,
        CASE TRIM(email_enviado)
            WHEN 'Sim' THEN TRUE
            WHEN 'Não' THEN FALSE
        END AS email_enviado_indicador,
        SAFE.PARSE_DATE('%d/%m/%Y', TRIM(data_entrada)) AS entrada_data,
        SAFE_CAST(TRIM(meta_entrega) AS INT64) AS meta_entrega,
        SAFE.PARSE_DATE('%d/%m/%Y', TRIM(data_saida)) AS saida_data,
        planilha_ano
    FROM controle_demanda
)

SELECT *
FROM tratado
-- Linhas sem número de referência e sem data de entrada não são demandas: são linhas vazias
-- ou a linha-modelo da planilha ("LINHA MESTRE NÃO DELETAR").
WHERE NOT (numero_referencia IS NULL AND entrada_data IS NULL)
