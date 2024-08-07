{{
    config(
        materialized='ephemeral',
    )
}}
-- ATTENTION!
-- THIS TABLE HAS DUPLICATED VALUES DUE TO THE FACT THAT A REPORT CAN HAVE MULTIPLES, ORGAOS, XPTOS, ASSUNTOS...
-- YOU NEED TO USE "DISTINCT" OR OTHER WAY TO ENSURE UNIQUENESS WHEN REFERENCES THIS TABLE.
-- USING THE RANKING = 1 WILL ENSURE THAT THE INFO IS THE NEWEST (THE MOST RECENT FILE FOUND IN BUCKET)
WITH stg_denuncias AS (
    SELECT
        denuncia_numero AS numero_denuncia, -- STRING
        CAST(denuncia_id AS INT) AS id_denuncia,
        denuncia_parent_numero AS numero_denuncia_parent, -- STRING
        CAST(denuncia_parent_id AS INT) AS id_denuncia_parent, -- verificar relação com a denúncia
        DATETIME(datetime_denuncia) AS data_denuncia,
        DATETIME(datetime_difusao) AS data_difusao,
        CAST(denuncia_imediata AS INT) AS denuncia_imediata,
        -- Clean and format address fields
        CAST(NULL AS STRING) AS tipo_logradouro, -- >>>>>> den_logr_tp > FALTA WOODY INSERIR NA API
        -- for now, the "endereco" field returns the full address
        TRIM(REGEXP_REPLACE(endereco, r'\s+', ' ')) AS logradouro, -- >>>>>> den_logr_ds > FALTA WOODY INSERIR NA API
        CAST(NULL AS STRING) AS numero_logradouro, -- >>>>>> den_logr_num > FALTA WOODY INSERIR NA API
        CAST(NULL AS STRING) AS complemento_logradouro,  -- >>>>>> den_logr_cmpl > FALTA WOODY INSERIR NA API
        TRIM(REGEXP_REPLACE(bairro, r'\s+', ' ')) AS bairro,
        CAST(NULL AS STRING) AS subbairro, -- >>>>>> den_logr_subbairro > FALTA WOODY INSERIR NA API
        CAST(NULL AS STRING) AS cep_logradouro,  -- >>>>>> den_logr_ds > FALTA WOODY INSERIR NA API
        CAST(NULL AS STRING) AS referencia_logradouro, -- >>>>>> den_logr_ref > FALTA WOODY INSERIR NA API
        TRIM(REGEXP_REPLACE(municipio, r'\s+', ' ')) AS municipio,
        TRIM(estado) AS estado,
        CAST(latitude AS FLOAT64) AS latitude,
        CAST(longitude AS FLOAT64) as longitude,
        relato,  -- STRING
        CAST(xpto_id AS INT) AS id_xpto,
        xpto_nome AS descricao_xpto, -- STRING
        CAST(orgao_id AS INT) AS id_orgao,
        orgao_nome AS nome_orgao, -- STRING
        orgao_tipo AS tipo_orgao, -- STRING
        CAST(NULL AS STRING) AS id_classe, -- >>>>>> cla_cd > FALTA WOODY INSERIR NA API
        assunto_classe AS descricao_classe, -- STRING
        CAST(NULL AS INT) AS assunto_principal, -- >>>>>> ass_principal > FALTA WOODY INSERIR NA API
        CAST(NULL AS STRING) AS id_tipo, -- >>>>>> tpa_cd > FALTA WOODY INSERIR NA API
        assunto_tipo AS descricao_tipo, -- STRING
        CAST(NULL AS INT) AS id_envolvido, -- env_cd >>>>>> FALTA WOODY INSERIR NA API
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
        denuncia_status AS status_denuncia,	 -- STRING
        REGEXP_EXTRACT(
            _FILE_NAME,
            r'(\d{8}_\d{6}_\d{6})_report_disque_denuncia\.csv$'
        ) AS nome_arquivo
    FROM
         `dd-teste.disque_denuncia.denuncias`
), denuncias_ranking AS (
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
SELECT
    *
FROM
    denuncias_ranking