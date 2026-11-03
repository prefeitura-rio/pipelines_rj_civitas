{{ config(
    severity = 'warn',
    warn_if = '> 0',
    error_if = '> 50'
) }}

-- Verifica se há câmeras ativas com locais diferentes em um mesmo código de ponto de coleta
SELECT codigo_ponto_coleta, origem_equipamento, COUNT(DISTINCT local)
FROM {{ ref('stg_equipamento') }}
GROUP BY codigo_ponto_coleta, origem_equipamento 
HAVING COUNT(DISTINCT local) > 1
