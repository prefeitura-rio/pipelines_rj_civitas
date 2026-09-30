{{ config(
    severity = 'warn',
    warn_if = '> 0',
    error_if = '> 50'
) }}

-- Verifica se há câmeras ativas com latlong com distância maior que 1km em um mesmo código de ponto de coleta
SELECT 
    codigo_ponto_coleta,
    geography,
    LAG(geography) OVER (PARTITION BY codigo_ponto_coleta ORDER BY latitude) AS lag_geo 
FROM {{ ref('equipamento') }}
WHERE status_ativo = TRUE AND COALESCE(codigo_ponto_coleta, '') != ''
QUALIFY ST_DISTANCE(geography, lag_geo) > 1000