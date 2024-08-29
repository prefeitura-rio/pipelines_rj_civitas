{{
    config(
        materialized='ephemeral'
    )
}}
-- CREATE OR REPLACE TABLE `rj-civitas-dev.disque_denuncia_staging.dev_denuncias_hist` AS
-- Cleaning and transformation.
WITH stg_denuncias AS (
  SELECT
    -- standardizing numero_denuncia
    CONCAT(
      den_numero,
      '.',
      EXTRACT(MONTH FROM DATETIME(den_dt_rec)),
      '.',
      EXTRACT(YEAR FROM DATETIME(den_dt_rec))) AS numero_denuncia,
    den_cd AS id_denuncia,
    '' AS numero_denuncia_parent,
    '' AS id_denuncia_parent,
    -- Convert datetime fields to DATETIME
    DATETIME(den_dt_rec) AS data_denuncia,
    DATETIME(den_dt_alt) AS data_difusao,
    den_imediata AS denuncia_imediata,
    -- Clean and format address fields
    TRIM(den_logr_tp) AS tipo_logradouro,
    TRIM(REGEXP_REPLACE(den_logr_ds, r'\s+', ' ')) AS logradouro,
    TRIM(REGEXP_REPLACE(den_logr_num, r'\s+', ' ')) AS numero_logradouro,
    TRIM(REGEXP_REPLACE(den_logr_cmpl, r'\s+', ' ')) AS complemento_logradouro,
    TRIM(REGEXP_EXTRACT(den_logr_bairro, r'^[^/]+')) AS bairro_logradouro,
    TRIM(REGEXP_REPLACE(den_logr_subbairro, r'[\[\]/\s]+', ' ')) AS subbairro_logradouro,
    TRIM(REGEXP_REPLACE(den_logr_cep, r'\s+', ' ')) AS cep_logradouro,
    TRIM(REGEXP_REPLACE(den_loc_ref, r'\s+', ' ')) AS referencia_logradouro,
    TRIM(REGEXP_REPLACE(den_logr_mun, r'\s+', ' ')) AS municipio,
    TRIM(den_logr_uf) AS estado,
    CAST(den_gps_lat AS FLOAT64) AS latitude,
    CAST(den_gps_long AS FLOAT64) as longitude,
    TRIM(den_texto) AS relato,
    dxp_xpt_cd AS id_xpto,
    TRIM(xpt_ds) AS nome_xpto,
    dex_ext_cd AS id_orgao,
    TRIM(ext_ds) AS nome_orgao,
    (CASE dex_dit_cd
      WHEN '1' THEN 'OPERACIONAL'
      WHEN '2' THEN 'INFORMATIVA'
      ELSE NULL
      END) AS tipo_orgao,
    ass_cla_cd AS id_classe,
    TRIM(cla_ds) AS descricao_classe,
    CAST(ass_principal AS INT) AS assunto_principal,
    ass_tpa_cd AS id_tipo,
    TRIM(tpa_ds) AS descricao_tipo,
    env_cd AS id_envolvido,
    -- Clean and format involved person's details
    TRIM(REGEXP_REPLACE(env_nome, r'\s+', ' ')) AS nome_envolvido,
    TRIM(REGEXP_REPLACE(env_vulgo, r'\s+', ' ')) AS vulgo_envolvido,
    TRIM(REGEXP_REPLACE(env_sexo, r'\s+', ' ')) AS sexo_envolvido,
    CAST(env_idade AS INT) AS idade_envolvido,
    TRIM(REGEXP_REPLACE(pel_ds, r'\s+', ' ')) AS pele_envolvido,
    TRIM(REGEXP_REPLACE(est_ds, r'\s+', ' ')) AS estatura_envolvido,
    TRIM(REGEXP_REPLACE(olh_ds, r'\s+', ' ')) AS olhos_envolvido,
    TRIM(REGEXP_REPLACE(cab_ds, r'\s+', ' ')) AS cabelos_envolvido,
    TRIM(REGEXP_REPLACE(prt_ds, r'\s+', ' ')) AS porte_envolvido,
    TRIM(REGEXP_REPLACE(env_caract, r'\s+', ' ')) AS outras_caracteristicas_envolvido,
    '' AS status_denuncia,
    -- Extract source file name for tracking the data origin
    REGEXP_EXTRACT(
        den._FILE_NAME,
        r'(\d{8}_\d{6}_\d{6})_.*\.csv$'
    ) AS nome_arquivo
  FROM
    `rj-civitas.disque_denuncia_staging.denuncias_historico` den
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.xpto_denuncia` dxp ON den_cd = dxp_den_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.xptos` xpt ON dxp_xpt_cd = xpt_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.difusao_externa` dex ON den_cd = dex_den_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.orgaos_externos` oex ON dex_ext_cd = ext_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.assuntos_denuncias` ass ON den_cd = ass_den_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.assuntos_classes` cla ON ass_cla_cd = cla_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.assuntos_tipos` tpa ON ass_tpa_cd = tpa_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.envolvidos` env ON den_cd = env_den_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.envolvidos_pele` pel ON env_pele = pel_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.envolvidos_estatura` est ON env_estatura = est_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.envolvidos_olhos` olh ON env_olhos = olh_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.envolvidos_cabelo` cab ON env_cabelo = cab_cd
  LEFT JOIN
    `rj-civitas.disque_denuncia_staging.envolvidos_porte` prt ON env_porte = prt_cd
),
-- Rank the 'denuncias' to identify the most recent entry for each unique denuncia
denuncias_ranking AS (
  SELECT
    *,
    -- 2024-07-15 is the max date of data in denuncias_historico
    DATETIME('2024-07-15 00:00:00.000000') AS timestamp_insercao,
    DENSE_RANK() OVER(
        PARTITION BY
            id_denuncia
        ORDER BY
            id_denuncia,
            nome_arquivo DESC,
            data_difusao DESC
    ) AS ranking
  FROM
    stg_denuncias
),
denuncias_distinct AS (
  SELECT DISTINCT
    numero_denuncia,
    id_denuncia,
    data_denuncia,
    data_difusao,
    tipo_logradouro,
    logradouro,
    numero_logradouro,
    complemento_logradouro,
    bairro_logradouro,
    subbairro_logradouro,
    cep_logradouro,
    referencia_logradouro,
    municipio,
    estado,
    latitude,
    longitude,
    relato,
    status_denuncia,
    timestamp_insercao
  FROM
    denuncias_ranking
  -- Filter to keep only the most recent ranking
  WHERE
    ranking = 1
),
-- CTES for get distinct values and create and an ARRAY_AGG column
xptos_distinct AS (
  SELECT DISTINCT
    id_denuncia,
    id_xpto,
    nome_xpto
  FROM
    denuncias_ranking
  WHERE
    ranking = 1
),
xptos_agg AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(
      STRUCT(
        id_xpto AS id,
        nome_xpto AS nome)
      ) AS xptos
  FROM
    xptos_distinct
  GROUP BY
    id_denuncia
),
orgaos_distinct AS (
  SELECT DISTINCT
    id_denuncia,
    id_orgao,
    nome_orgao,
    tipo_orgao
  FROM
    denuncias_ranking
  WHERE
    ranking = 1
),
orgaos_agg AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(
      STRUCT(
        id_orgao AS id,
        nome_orgao AS nome,
        tipo_orgao AS tipo)
      ) AS orgaos
  FROM
    orgaos_distinct
  GROUP BY
    id_denuncia
),
classes_distinct AS (
  SELECT DISTINCT
    id_denuncia,
    id_classe,
    descricao_classe AS classe,
  FROM
    denuncias_ranking
  WHERE
    ranking = 1
),
tipos_distinct AS (
  SELECT DISTINCT
    id_denuncia,
    id_classe,
    id_tipo,
    descricao_tipo AS tipo,
    assunto_principal
  FROM
    denuncias_ranking
  WHERE
    ranking = 1
),
tipos_agg AS (
  SELECT
    id_denuncia,
    id_classe,
    ARRAY_AGG(
      STRUCT(
        tpo.id_tipo,
        tpo.tipo,
        tpo.assunto_principal
      )
    ) AS tipos
  FROM
    tipos_distinct tpo
  GROUP BY
    id_denuncia,
    id_classe
),
classes_tipos_agg AS (
  SELECT
    cla.id_denuncia,
    ARRAY_AGG(
      STRUCT(
        cla.id_classe,
        cla.classe,
        tpo.tipos
      )
    ) AS assuntos
  FROM
    classes_distinct cla
  LEFT JOIN
    tipos_agg tpo ON cla.id_denuncia = tpo.id_denuncia AND cla.id_classe = tpo.id_classe
  GROUP BY
    id_denuncia
),
envolvidos_distinct AS (
  SELECT DISTINCT
    id_denuncia,
    id_envolvido AS id,
    nome_envolvido AS nome,
    vulgo_envolvido AS vulgo,
    sexo_envolvido AS sexo,
    idade_envolvido AS idade,
    pele_envolvido AS pele,
    estatura_envolvido AS estatura,
    porte_envolvido AS porte,
    cabelos_envolvido AS cabelos,
    olhos_envolvido AS olhos,
    outras_caracteristicas_envolvido AS outras_caracteristicas
  FROM
    denuncias_ranking
  WHERE
    ranking = 1
),
envolvidos_agg AS (
  SELECT
    id_denuncia,
    ARRAY_AGG(
      STRUCT(
        id,
        nome,
        vulgo,
        sexo,
        idade,
        pele,
        estatura,
        porte,
        cabelos,
        olhos,
        outras_caracteristicas
      )
    ) AS envolvidos
  FROM
    envolvidos_distinct
  GROUP BY
    id_denuncia
)
-- final query
SELECT
  den.numero_denuncia,
  den.id_denuncia,
  den.data_denuncia,
  den.data_difusao,
  den.tipo_logradouro,
  den.logradouro,
  den.numero_logradouro,
  den.complemento_logradouro,
  den.bairro_logradouro,
  den.subbairro_logradouro,
  den.cep_logradouro,
  den.referencia_logradouro,
  den.municipio,
  den.estado,
  den.latitude,
  den.longitude,
  xpt.xptos,
  org.orgaos,
  cla.assuntos,
  den.relato,
  env.envolvidos,
  den.status_denuncia,
  den.timestamp_insercao
FROM
  denuncias_distinct den
LEFT JOIN
  xptos_agg xpt ON den.id_denuncia = xpt.id_denuncia
LEFT JOIN
  orgaos_agg org ON den.id_denuncia = org.id_denuncia
LEFT JOIN
  classes_tipos_agg cla ON den.id_denuncia = cla.id_denuncia
LEFT JOIN
  envolvidos_agg env ON den.id_denuncia = env.id_denuncia
