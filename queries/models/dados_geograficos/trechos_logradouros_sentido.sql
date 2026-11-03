{{
    config(
        materialized='table'
    )
}}

WITH logradouros AS (
  SELECT
    *,
    ST_STARTPOINT(geometry) AS inicio,
    ST_ENDPOINT(geometry) AS fim
  FROM {{ source('datario', 'logradouro') }}
),

direcao AS (
  SELECT
    *,
    
    CASE
      WHEN sentido_unico = 'FT' THEN inicio
      WHEN sentido_unico = 'TF' THEN fim
    END AS origem,

    CASE
      WHEN sentido_unico = 'FT' THEN fim
      WHEN sentido_unico = 'TF' THEN inicio
    END AS destino

  FROM logradouros
),

azimute AS (
  SELECT
    *,
    ST_AZIMUTH(origem, destino) AS angulo
  FROM direcao
  WHERE sentido_unico IN ('FT', 'TF')
),

reta AS (
  SELECT
    *,
    
    ST_MAKELINE(
      destino,

      ST_GEOGPOINT(
        ST_X(destino)
          + (20.0 / 111.32)
          * SIN(angulo)
          / COS(
              ST_Y(destino) * ACOS(-1) / 180
            ),

        ST_Y(destino)
          + (20.0 / 111.32)
          * COS(angulo)
      )
    ) AS linha

  FROM azimute
),

proximas_ruas AS (
  SELECT
    r.*,
    l.nome_completo AS proxima_rua

  FROM reta r

  JOIN logradouros l
    ON ST_INTERSECTS(
      r.linha,
      l.geometry
    )

  WHERE LOWER(l.nome_completo) != LOWER(r.nome_completo)

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY r.id_trecho
    ORDER BY ST_DISTANCE(
      r.destino,
      ST_CLOSESTPOINT(
        l.geometry,
        r.destino
      )
    )
  ) = 1
),
final as (
SELECT
  l.* EXCEPT(inicio, fim),
  
  CASE
    WHEN COALESCE(l.sentido_unico, 'NULL') NOT IN ('FT', 'TF')
      THEN 'INDEFINIDO'

    ELSE p.proxima_rua
  END AS proxima_rua

FROM logradouros l

LEFT JOIN proximas_ruas p
  ON l.id_trecho = p.id_trecho
)

SELECT 
id_bairro,
id_logradouro,
id_trecho,
nome_completo,
nome_bairro,
sentido_unico, 
tipo_trecho,
UPPER(TRIM(proxima_rua)) AS sentido,
geometry
from final
