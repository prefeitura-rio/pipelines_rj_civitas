{{
    config(
        materialized='view',
        unique_key='id_denuncia',
        partition_by={
            "field": "data_denuncia",
            "data_type": "datetime",
            "granularity": "month",
        }
    )
}}
-- CTE for retrieving id_denuncia from denuncias_api table (newest)
WITH id_new_reports AS (
  SELECT DISTINCT
    id_denuncia
  FROM
    `rj-civitas.disque_denuncia.denuncias_api`
)
-- Joining tables
-- Keeping the most recent data when id_denuncia is in the denuncias_api and denuncias_historico tables
SELECT
  *
FROM
  `rj-civitas.disque_denuncia.denuncias_historico`
WHERE
  id_denuncia NOT IN (SELECT id_denuncia FROM id_new_reports)

UNION ALL

SELECT
  *
FROM
  `rj-civitas.disque_denuncia.denuncias_api`
