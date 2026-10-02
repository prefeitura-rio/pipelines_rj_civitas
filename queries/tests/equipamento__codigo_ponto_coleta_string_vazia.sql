{{ config(
    severity = 'warn',
    warn_if = '> 0',
    error_if = '> 50'
) }}

-- Verifica se há câmeras ativas com latlong com código de ponto de coleta como string vazia
SELECT 
    codigo_ponto_coleta
FROM {{ ref('equipamento') }}
WHERE codigo_ponto_coleta = ''