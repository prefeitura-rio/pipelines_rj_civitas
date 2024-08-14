{{
    config(
        materialized='table',
        unique_key='id_denuncia',
        partition_by={
            "field": "data_denuncia",
            "data_type": "datetime",
            "granularity": "month",
        }
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
