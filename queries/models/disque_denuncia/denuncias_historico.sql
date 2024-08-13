{{
    config(
        materialized='table',
        unique_key='id_denuncia',
        partition_by={
            "field": "data_denuncia",
            "data_type": "date",
            "granularity": "month",
        }
    )
}}
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
        den_logr_tp AS tipo_logradouro,
        TRIM(REGEXP_REPLACE(den_logr_ds, r'\s+', ' ')) AS logradouro,
        TRIM(REGEXP_REPLACE(den_logr_num, r'\s+', ' ')) AS numero_logradouro,
        TRIM(REGEXP_REPLACE(den_logr_cmpl, r'\s+', ' ')) AS complemento_logradouro,
        TRIM(REGEXP_EXTRACT(den_logr_bairro, r'^[^/]+')) AS bairro,
        TRIM(REGEXP_REPLACE(den_logr_subbairro, r'[\[\]/\s]+', ' ')) AS subbairro,
        TRIM(REGEXP_REPLACE(den_logr_cep, r'\s+', ' ')) AS cep_logradouro,
        TRIM(REGEXP_REPLACE(den_loc_ref, r'\s+', ' ')) AS referencia_logradouro,
        TRIM(REGEXP_REPLACE(den_logr_mun, r'\s+', ' ')) AS municipio,
        TRIM(den_logr_uf) AS estado,
        CAST(den_gps_lat AS FLOAT64) AS latitude,
        CAST(den_gps_long AS FLOAT64) as longitude,
        den_texto AS relato,
        dxp_xpt_cd AS id_xpto,
        '' AS nome_xpto, -- FALTA CRIAR TABELA XPTOS
        dex_ext_cd AS id_orgao,
        ext_ds AS nome_orgao,
        (CASE dex_dit_cd
          WHEN '1' THEN 'OPERACIONAL'
          WHEN '2' THEN 'INFORMATIVA'
          ELSE NULL
          END) AS tipo_orgao,
        ass_cla_cd AS id_classe,
        cla_ds AS descricao_classe,
        CAST(ass_principal AS INT) AS assunto_principal,
        ass_tpa_cd AS id_tipo,
        tpa_ds AS descricao_tipo,
        env_cd AS id_envolvido,
        -- Clean and format involved person's details
        TRIM(REGEXP_REPLACE(env_nome, r'\s+', ' ')) AS nome_envolvido,
        TRIM(REGEXP_REPLACE(env_vulgo, r'\s+', ' ')) AS vulgo_envolvido,
        TRIM(REGEXP_REPLACE(env_sexo, r'\s+', ' ')) AS sexo_envolvido,
        CAST(env_idade AS INT) AS idade_envolvido,
        -- >>>>>>>>>>>>>>> TABELA AUXILIAR 'CARACTERISTICAS' FALTANDO <<<<<<<<<<<<<<<<<
        -- TRIM(REGEXP_REPLACE(env_pele, r'\s+', ' ')) AS pele_envolvido,
        -- TRIM(REGEXP_REPLACE(env_estatura, r'\s+', ' ')) AS estatura_envolvido,
        -- TRIM(REGEXP_REPLACE(env_olhos, r'\s+', ' ')) AS olhos_envolvido,
        -- TRIM(REGEXP_REPLACE(env_cabelo, r'\s+', ' ')) AS cabelos_envolvido,
        -- TRIM(REGEXP_REPLACE(env_porte, r'\s+', ' ')) AS porte_envolvido,
        -- TRIM(REGEXP_REPLACE(env_caract, r'\s+', ' ')) AS outras_caracteristicas_envolvido,

        -- >>>>>>>>>>>>> APAGAR <<<<<<<<<<<<<<<<
        '' AS pele_envolvido,
        '' AS estatura_envolvido,
        '' AS olhos_envolvido,
        '' AS cabelos_envolvido,
        '' AS porte_envolvido,
        '' AS outras_caracteristicas_envolvido,

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
),
-- Rank the 'denuncias' to identify the most recent entry for each unique denuncia
denuncias_ranking AS (
    SELECT
        *,
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
)
-- select * from denuncias_ranking
-- Select and aggregate the most recent 'denuncias' data
SELECT
  dr.numero_denuncia,
  dr.id_denuncia,
  dr.data_denuncia,
  dr.data_difusao,
  dr.tipo_logradouro,
  dr.logradouro,
  dr.numero_logradouro,
  dr.complemento_logradouro,
  dr.bairro AS bairro_logradouro,
  dr.subbairro AS subbairro_logradouro,
  dr.cep_logradouro,
  dr.referencia_logradouro,
  dr.municipio,
  dr.estado,
  dr.latitude,
  dr.longitude,
  ARRAY_AGG(
    STRUCT(
      dr.id_xpto AS id,
      dr.nome_xpto AS nome)
    ) AS xptos,
  ARRAY_AGG(
    STRUCT(
      dr.id_orgao AS id,
      dr.nome_orgao AS nome,
      dr.tipo_orgao AS tipo
    )
  ) AS orgaos,
  ARRAY_AGG(
    STRUCT(
      dr.id_classe,
      dr.descricao_classe AS classe,
      dr.id_tipo,
      dr.descricao_tipo AS tipo,
      dr.assunto_principal
    )
  ) AS assuntos,
  dr.relato,
  ARRAY_AGG(
    STRUCT(
      dr.id_envolvido AS id,
      dr.nome_envolvido AS nome,
      dr.vulgo_envolvido AS vulgo,
      dr.sexo_envolvido AS sexo,
      dr.idade_envolvido AS idade,
      dr.pele_envolvido AS pele,
      dr.estatura_envolvido AS estatura,
      dr.porte_envolvido AS porte,
      dr.cabelos_envolvido AS cabelos,
      dr.olhos_envolvido AS olhos,
      dr.outras_caracteristicas_envolvido AS outras_caracteristicas
    )
  ) AS envolvidos,
  dr.status_denuncia
FROM
    denuncias_ranking dr
-- Filter to keep only the most recent ranking
WHERE
    ranking = 1
GROUP BY
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
  status_denuncia