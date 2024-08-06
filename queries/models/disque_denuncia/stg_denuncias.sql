{{
    config(
        materialized='table',
    )
}}
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
        TRIM(REGEXP_REPLACE(endereco, r'\s+', ' ')) AS endereco,
        TRIM(REGEXP_REPLACE(bairro, r'\s+', ' ')) AS bairro,
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
        assunto_classe AS classe_assunto, -- STRING
        assunto_tipo AS tipo_assunto, -- STRING
        envolvido_nome AS nome_envolvido, -- STRING
        envolvido_vulgo AS vulgo_envolvido, -- STRING
        envolvido_sexo AS sexo_envolvido, -- STRING
        CAST(envolvido_idade AS INT) AS idade_envolvido,
        envolvido_pele AS pele_envolvido, -- STRING
        envolvido_estatura AS estatura_envolvido, -- STRING
        envolvido_porte AS porte_envolvido, -- STRING
        envolvido_cabelos AS cabelos_envolvido,	 -- STRING
        envolvido_olhos AS olhos_envolvido,	 -- STRING
        envolvido_outras_caracteristicas AS outras_caracteristicas_envolvido,	 -- STRING
        denuncia_status AS status_denuncia,	 -- STRING
        REGEXP_EXTRACT(
            _FILE_NAME,
            r'(\d{8}_\d{6}_\d{6})_report_disque_denuncia\.csv$'
        ) AS nome_arquivo
    FROM
         `rj-civitas.disque_denuncia_staging.denuncias`
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