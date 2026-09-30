{{ config(
    severity = 'warn',
    warn_if = '> 0',
    error_if = '> 50'
) }}

-- Verifica se há câmeras ativas em bairros diferentes em um mesmo código de ponto de coleta
SELECT codigo_ponto_coleta, COUNT(DISTINCT bairro)
FROM {{ ref('equipamento') }}
WHERE status_ativo = TRUE 
GROUP BY codigo_ponto_coleta 
HAVING COUNT(DISTINCT bairro) > 1
