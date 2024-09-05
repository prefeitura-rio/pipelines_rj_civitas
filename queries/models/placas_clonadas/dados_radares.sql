{{
  config(
    materialized = 'table',
    cluster_by = ['camera_numero']
  )
}}

WITH cameras_cetrio AS (
    SELECT
        t2.camera_numero,
        t1.locequip,
        t1.bairro,
        t1.sentido,
        CAST(t1.latitude AS FLOAT64) AS latitude,
        CAST(t1.longitude AS FLOAT64) AS longitude,
        ST_GEOGPOINT(CAST(t1.longitude AS FLOAT64), CAST(t1.latitude AS FLOAT64)) AS geo_coordinates
    FROM `rj-cetrio.ocr_radar.equipamento` t1
    JOIN `rj-cetrio.ocr_radar.equipamento_codcet_to_camera_numero` t2
        ON t1.codcet = t2.codcet
)
SELECT 
    r.camera_numero, 
    r.placa, 
    r.tipoveiculo,
    r.velocidade,
    r.datahora,
    r.datahora_captura,
    ABS(TIMESTAMP_DIFF(r.datahora, r.datahora_captura, SECOND)) AS erro_datahora,
    EXTRACT(HOUR FROM r.datahora) AS hora,
    EXTRACT(DAYOFWEEK FROM r.datahora) AS dia_da_semana,
    c.bairro,
    CASE
        WHEN r.empresa = 'SPLICE' THEN r.camera_latitude
        ELSE c.latitude
    END AS latitude,
    CASE
        WHEN r.empresa = 'SPLICE' THEN r.camera_longitude
        ELSE c.longitude
    END AS longitude,
    (rc.camera_numero IS NOT NULL) AS tipo_confiavel
FROM `rj-cetrio.ocr_radar.readings_2024_06` r -- TODO: trocar a data para *
LEFT JOIN cameras_cetrio c
    ON r.camera_numero = c.camera_numero
LEFT JOIN `rj-civitas.dbt.radares_confiaveis` rc
    ON r.camera_numero = rc.camera_numero
