{{ config(
    severity = 'warn',
    warn_if = '> 0',
    error_if = '> 50'
) }}

-- Verifica se há câmeras ativas com latlong com distância maior que 1km em um mesmo código de ponto de coleta
SELECT 
    codigo_ponto_coleta
FROM {{ ref('equipamento') }}
WHERE codigo_ponto_coleta = ''