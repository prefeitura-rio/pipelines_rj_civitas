{{
  config(
    materialized = 'table',
    cluster_by = ['placa', 'pontos_clonagem']
  )
}}

WITH placa_tipos_confiavel AS (
  SELECT 
    vm.placa,
    COUNT(DISTINCT vm.tipo_confiavel) AS qtd_tipos_confiavel
  FROM `rj-civitas.placas_clonadas.dados_radares` vm
  INNER JOIN `rj-civitas.dbt.radares_confiaveis` rc
    ON vm.camera_numero = rc.camera_numero
  GROUP BY vm.placa
),

placa_tipos as (
  SELECT placa, COUNT(DISTINCT tipoveiculo) AS qtd_tipos
  FROM `rj-civitas.placas_clonadas.dados_radares`
  GROUP BY placa
),

cameras as (
    SELECT
      placa,
      camera_inicial AS camera,
      posicao_inicial AS posicao,
      velocidade_media,
      velocidade_maxima,
      tipos_diferentes,
      tipo_confiavel,
      camera_final AS camera2
    FROM `rj-civitas.placas_clonadas.identificacao_anomalias_placas`
  UNION ALL
    SELECT
      placa,
      camera_final AS camera,
      posicao_final AS posicao,
      velocidade_media,
      velocidade_maxima,
      tipos_diferentes,
      tipo_confiavel,
      camera_inicial AS camera2
    FROM `rj-civitas.placas_clonadas.identificacao_anomalias_placas`
),

-- Identifica radares com muitas vel. médias acima do esperado
cameras_validas as (
  SELECT 
    camera,
    COUNTIF(velocidade_media < velocidade_maxima OR tipos_diferentes) / count(*) AS porcentagem_coerente,
  FROM cameras
  group by camera
  having porcentagem_coerente > 0.7
),

velocidade_veiculo as (
  select placa, GREATEST(150, MAX(velocidade) + 30) AS velocidade_maxima
  from `rj-civitas.placas_clonadas.dados_radares`
  group by placa
),

-- 2. Inconsistências de Deslocamento e Mudanças de Tipo
inconsistencias_deslocamento AS (
  SELECT 
    c.placa,
  pt.qtd_tipos,
  ptc.qtd_tipos_confiavel,
  COUNTIF(camera in (select camera from cameras_validas) AND camera2 in (select camera from cameras_validas)) as qtd_inconsistencias_deslocamento,
  COUNTIF(c.tipos_diferentes AND c.tipo_confiavel) AS mudanca_tipo_confiavel,
  COUNTIF(c.tipos_diferentes) AS mudanca_tipo_total
FROM cameras c
inner join placa_tipos ptpp
  on c.placa = pt.placa
left join placa_tipos_confiavel ptc
  on c.placa = ptc.placa
where velocidade_media > velocidade_maxima
group by placa, qtd_tipos, qtd_tipos_confiavel
)

-- 3. Cálculo dos Pontos de Clonagem e Aplicação de Filtro
SELECT *
FROM (
  SELECT 
    i.placa,
    (ptc.qtd_tipos_confiavel - 1) / 3
    + i.qtd_inconsistencias_deslocamento / 10
    + i.mudanca_tipo_confiavel / 4
    + i.mudanca_tipo_total / 8 AS pontos_clonagem
  FROM inconsistencias_deslocamento i
  LEFT JOIN placa_tipos_confiavel ptc
    ON i.placa = ptc.placa
) AS subquery
WHERE pontos_clonagem > 1.0
