{{
    config(
        materialized='ephemeral'
    )
}}
-- Cleaning and transformation.
WITH stg_denuncias AS (
  SELECT
    denuncia_numero AS numero_denuncia,
    denuncia_id AS id_denuncia,
    denuncia_parent_numero AS numero_denuncia_parent,
    denuncia_parent_id AS id_denuncia_parent,
    -- Convert datetime fields to DATETIME
    DATETIME(datetime_denuncia) AS data_denuncia,
    DATETIME(datetime_difusao) AS data_difusao,
    CAST(denuncia_imediata AS INT) AS denuncia_imediata,
    -- Clean and format address fields
    tipo_logradouro,
    TRIM(REGEXP_REPLACE(descricao_logradouro, r'\s+', ' ')) AS logradouro,
    TRIM(REGEXP_REPLACE(numero_logradouro, r'\s+', ' ')) AS numero_logradouro,
    TRIM(REGEXP_REPLACE(complemento_logradouro, r'\s+', ' ')) AS complemento_logradouro,
    TRIM(REGEXP_EXTRACT(bairro, r'^[^/]+')) AS bairro,
    TRIM(REGEXP_REPLACE(subbairro, r'[\[\]/\s]+', ' ')) AS subbairro,
    TRIM(REGEXP_REPLACE(cep_logradouro, r'\s+', ' ')) AS cep_logradouro,
    TRIM(REGEXP_REPLACE(referencia_logradouro, r'\s+', ' ')) AS referencia_logradouro,
    TRIM(REGEXP_REPLACE(municipio, r'\s+', ' ')) AS municipio,
    TRIM(estado) AS estado,
    CAST(latitude AS FLOAT64) AS latitude,
    CAST(longitude AS FLOAT64) as longitude,
    relato,
    xpto_id AS id_xpto,
    xpto_nome AS nome_xpto,
    orgao_id AS id_orgao,
    orgao_nome AS nome_orgao,
    orgao_tipo AS tipo_orgao,
    assunto_classe_id AS id_classe,
    assunto_classe AS descricao_classe,
    CAST(assunto_principal AS INT) AS assunto_principal,
    assunto_tipo_id AS id_tipo,
    assunto_tipo AS descricao_tipo,
    envolvido_id AS id_envolvido,
    -- Clean and format involved person's details
    TRIM(REGEXP_REPLACE(envolvido_nome, r'\s+', ' ')) AS nome_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_vulgo, r'\s+', ' ')) AS vulgo_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_sexo, r'\s+', ' ')) AS sexo_envolvido,
    CAST(envolvido_idade AS INT) AS idade_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_pele, r'\s+', ' ')) AS pele_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_estatura, r'\s+', ' ')) AS estatura_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_olhos, r'\s+', ' ')) AS olhos_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_cabelos, r'\s+', ' ')) AS cabelos_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_porte, r'\s+', ' ')) AS porte_envolvido,
    TRIM(REGEXP_REPLACE(envolvido_outras_caracteristicas, r'\s+', ' ')) AS outras_caracteristicas_envolvido,
    denuncia_status AS status_denuncia,
    -- Extract source file name for tracking the data origin
    REGEXP_EXTRACT(
        _FILE_NAME,
        r'(\d{8}_\d{6}_\d{6})_.*\.csv$'
    ) AS nome_arquivo
  FROM
      `rj-civitas.disque_denuncia_staging.denuncias`
),
-- Rank the 'denuncias' to identify the most recent entry for each unique denuncia
denuncias_ranking AS (
  SELECT
    *,
    DATETIME(
      CONCAT(
        SUBSTR(nome_arquivo, 1, 4), '-',      -- Ano (YYYY)
        SUBSTR(nome_arquivo, 5, 2), '-',      -- Mês (MM)
        SUBSTR(nome_arquivo, 7, 2), ' ',      -- Dia (den)
        SUBSTR(nome_arquivo, 10, 2), ':',     -- Hora (HH)
        SUBSTR(nome_arquivo, 12, 2), ':',     -- Minuto (MM)
        SUBSTR(nome_arquivo, 14, 2), '.',     -- Segundo (SS)
        SUBSTR(nome_arquivo, 17, 6)           -- Microsegundos (FFFFFF)
      )
    ) AS timestamp_insercao,
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
    numero_denuncia_parent,
    id_denuncia_parent,
    data_denuncia,
    data_difusao,
    denuncia_imediata,
    tipo_logradouro,
    logradouro,
    numero_logradouro,
    complemento_logradouro,
    bairro,
    subbairro,
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
  WHERE ranking = 1
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
        id_denuncia,
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
  den.bairro AS bairro_logradouro,
  den.subbairro AS subbairro_logradouro,
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
