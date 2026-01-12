# Pipelines rj-civitas

Repositório de pipelines de dados da CIVITAS, responsável por processar e integrar e dados.

## 📋 Índice

- [Sobre o Projeto](#sobre-o-projeto)
- [Arquitetura](#arquitetura)
  - [Stack Tecnológica](#stack-tecnológica)
  - [Padrão Arquitetural de Pipelines](#padrão-arquitetural-de-pipelines)
  - [Padrão Arquitetural de Queries (DBT)](#padrão-arquitetural-de-queries-dbt)
- [Setup do Ambiente de Desenvolvimento](#setup-do-ambiente-de-desenvolvimento)
- [Como Rodar o Projeto](#como-rodar-o-projeto)
- [Desenvolvimento](#desenvolvimento)

---

## Sobre o Projeto

O **Pipelines RJ Civitas** é um conjunto de pipelines de dados construídos com **Prefect** e **DBT** para orquestração e transformação de dados.

### Principais Funcionalidades

- **Extração e Carga (ELT)**: Coleta de dados de múltiplas fontes (APIs, web scraping, arquivos XML)
- **Transformação de Dados**: Processamento e normalização com DBT
- **Integração de Relatórios**: Consolidação de dados de diferentes fontes (Disque Denúncia, Fogo Cruzado, 1746, redes sociais)
- **Alertas e Notificações**: Sistema de alertas via Discord
- **Enriquecimento com IA**: Processamento de relatórios com LLM para análise de contexto
- **Georreferenciamento**: Enriquecimento de dados com coordenadas geográficas

### Fontes de Dados

- **Disque Denúncia**: Denúncias anônimas de crimes
- **Fogo Cruzado**: Ocorrências de violência armada
- **1746**: Central de atendimento ao cidadão (telemetria e relatórios)
- **Redes Sociais**: Twitter e Telegram
- **Câmeras e Radares**: Leituras de placas através de câmeras e radares

---

## Arquitetura

### Stack Tecnológica

- **Python 3.10**: Linguagem principal
- **Prefect 1.4.1**: Orquestração de workflows
- **DBT (Data Build Tool)**: Transformação de dados no BigQuery
- **Google BigQuery**: Data warehouse
- **Google Cloud Storage**: Armazenamento de objetos
- **Docker**: Containerização
- **Poetry**: Gerenciamento de dependências

### Padrão Arquitetural de Pipelines

Os pipelines seguem o padrão **ETL (Extract, Transform, Load)** orquestrado pelo Prefect, com a seguinte estrutura modular:

#### Estrutura de Diretórios

```
pipelines/
├── <nome_do_dominio>/           # Ex: fogo_cruzado, disque_denuncia
│   ├── __init__.py
│   ├── <nome_do_pipeline>/      # Ex: extract_load, alertas
│   │   ├── __init__.py
│   │   ├── flows.py             # Definição do flow Prefect
│   │   ├── tasks.py             # Tarefas individuais do pipeline
│   │   └── schedules.py         # Agendamentos do flow
├── constants.py                  # Constantes globais
├── flows.py                      # Registro de todos os flows
├── templates/                    # Templates reutilizáveis
└── utils/                        # Utilitários compartilhados
```

#### Anatomia de um Pipeline

**1. Flow (`flows.py`)**

```python
from prefect import Parameter, case
from prefeitura_rio.pipelines_utils.custom import Flow
from pipelines.constants import FLOW_RUN_CONFIG, FLOW_STORAGE

with Flow(
    name="CIVITAS: Nome do Pipeline",
    state_handlers=[
        handler_inject_bd_credentials,      # Injeta credenciais do BigQuery
        handler_skip_if_running,            # Evita execuções concorrentes
        handler_notify_on_failure,          # Notifica falhas no Discord
    ],
) as meu_pipeline:
    # 1. Definir parâmetros
    PROJECT_ID = Parameter("project_id", default="rj-civitas")
    DATASET_ID = Parameter("dataset_id", default="meu_dataset")

    # 2. Executar tarefas
    dados_extraidos = extrair_dados(parametros)
    dados_transformados = transformar_dados(dados_extraidos)
    load_result = carregar_dados(dados_transformados, PROJECT_ID, DATASET_ID)

    # 3. Materialização com DBT (opcional)
    with case(task=MATERIALIZE_AFTER_DUMP, value=True):
        materialization_flow = create_flow_run(
            flow_name=constants.FLOW_NAME_DBT_TRANSFORM.value,
            parameters={"select": DATASET_ID}
        )

# Configuração do flow
meu_pipeline.storage = FLOW_STORAGE
meu_pipeline.run_config = FLOW_RUN_CONFIG
meu_pipeline.schedule = meu_schedule
```

**2. Tasks (`tasks.py`)**

```python
from prefect import task
from prefeitura_rio.pipelines_utils.logging import log

@task(max_retries=3, retry_delay=timedelta(seconds=10))
def extrair_dados(parametros):
    """
    Extrai dados de uma fonte externa.

    Args:
        parametros: Configuração da extração

    Returns:
        dict: Dados extraídos
    """
    log("Iniciando extração de dados...")
    # Lógica de extração
    return dados
```

**3. Schedules (`schedules.py`)**

```python
from datetime import timedelta
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

meu_schedule = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=30),
            parameter_defaults={
                "project_id": "rj-civitas",
                "dataset_id": "meu_dataset",
            },
        )
    ]
)
```

#### State Handlers

State handlers são executados em diferentes estágios do flow:

- **`handler_inject_bd_credentials`**: Injeta automaticamente credenciais do BigQuery
- **`handler_skip_if_running`**: Previne execuções concorrentes do mesmo flow
- **`handler_notify_on_failure`**: Envia notificações no Discord em caso de falha
- **`handler_initialize_sentry`**: Integração com Sentry para monitoramento de erros

---

### Padrão Arquitetural de Queries (DBT)

As transformações de dados utilizam **DBT (Data Build Tool)**.

#### Estrutura de Diretórios

```
queries/
├── dbt_project.yml              # Configuração do projeto DBT
├── profiles.yml                 # Perfis de conexão (dev/prod)
├── packages.yml                 # Pacotes DBT externos
├── models/                      # Modelos de transformação
│   ├── <dominio>/              # Ex: fogo_cruzado, disque_denuncia
│   │   ├── schema.yml          # Documentação e testes
│   │   └── *.sql               # Models
│   └── schema.yml              # Documentação global
├── macros/                      # Macros reutilizáveis
├── seeds/                       # Dados estáticos (CSVs)
├── snapshots/                   # Snapshots de SCD Type 2
└── tests/                       # Testes customizados
```

#### Camadas de Modelagem

**1. Staging Layer (datasets `*_staging`)**

Os dados brutos são carregados pelos pipelines em datasets separados com sufixo `_staging` no BigQuery:
- `fogo_cruzado_staging` - dados brutos do Fogo Cruzado
- `disque_denuncia_staging` - dados brutos do Disque Denúncia
- `cerco_digital_staging` - dados brutos de câmeras e radares

**2. Intermediate Layer (opcional)**

Para lógicas complexas que serão reutilizadas:
- CTEs modulares
- Agregações intermediárias
- Joins complexos

**3. Mart Layer (models finais)**

Tabelas prontas para consumo nos datasets finais:
- Dados desnormalizados e enriquecidos
- Agregações de negócio
- Documentação completa no `schema.yml`

```sql
-- models/fogo_cruzado/ocorrencias.sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='id_ocorrencia',
        partition_by={
            "field": "data_ocorrencia",
            "data_type": "datetime",
            "granularity": "month",
        },
        cluster_by=['timestamp_update']
    )
}}

WITH newest_data AS (
    SELECT * FROM {{ source('stg_fogo_cruzado', 'ocorrencias') }}
    {% if is_incremental() %}
        WHERE timestamp_insercao > (SELECT MAX(timestamp_update) FROM {{ this }})
    {% endif %}
)

SELECT
    id_ocorrencia,
    data_ocorrencia,
    endereco,
    -- transformações...
FROM newest_data
```

---

## Setup do Ambiente de Desenvolvimento

### Pré-requisitos

- **Python 3.10** (exatamente)
- **Poetry** >= 1.5
- **Git**
- **Google Cloud SDK** (gcloud CLI)
- **Docker** (opcional, para testes locais containerizados)

### 1. Clonar o Repositório

```bash
git clone https://github.com/prefeitura-rio/pipelines_rj_civitas.git
cd pipelines_rj_civitas
```

### 2. Instalar Dependências

#### Usando Poetry (recomendado)

```bash
# Instalar Poetry se ainda não tiver
curl -sSL https://install.python-poetry.org | python3 -

# Criar ambiente virtual e instalar dependências
poetry install

# Instalar dependências de desenvolvimento
poetry install --with dev

# Ativar ambiente virtual
poetry shell
```

#### Usando pip (alternativa)

```bash
# Criar ambiente virtual
python3.10 -m venv venv
source venv/bin/activate  # Linux/Mac
# ou
.\venv\Scripts\activate   # Windows

# Instalar dependências
pip install -U pip
pip install poetry
poetry install
```

### 3. Configurar Variáveis de Ambiente

Crie um arquivo `.env` na raiz do projeto:

```bash
# Ambiente
ENVIRONMENT=dev

# Variáveis de ambiente do Infisical
INFISICAL_ADDRESS=https://infisical-host
INFISICAL_TOKEN=token

# BigQuery
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Redis (para cache - opcional)
REDIS_HOST=localhost
REDIS_PORT=6379

```

### 4. Configurar Pre-commit Hooks

```bash
poetry run pre-commit install
```

---

## Como Rodar o Projeto

### Rodar Pipelines Localmente (Modo Dev)

#### 1. Iniciar Prefect Server (em um terminal separado)

```bash
poetry shell
prefect backend server
prefect server start
```

Acesse a UI em: http://localhost:8080

#### 2. Iniciar Prefect Agent (em outro terminal)

```bash
poetry shell
prefect agent local start --label civitas
```

#### 3. Registrar Flows no Prefect
Caso não tenha criado um projeto
```bash
prefect create project dev
```

```bash
poetry shell
prefect register --project dev -m pipelines.<dominio>.<nome_pipeline>.flows
```

#### 4. Rodar o flow na UI
1. Acesse http://localhost:8080
2. Navegue até "Flows"
3. Selecione o flow desejado
4. Clique em "Quick Run" ou "Run" para configurar parâmetros


### Rodar Transformações DBT

#### Executar Todos os Modelos

```bash
cd queries
dbt run
```

#### Executar Modelos Específicos

```bash
# Rodar apenas o domínio fogo_cruzado
dbt run --select fogo_cruzado

# Rodar modelo específico
dbt run --select fogo_cruzado.ocorrencias

# Rodar modelo e suas dependências
dbt run --select +fogo_cruzado.ocorrencias

# Rodar modelo e seus descendentes
dbt run --select fogo_cruzado.ocorrencias+
```

#### Executar Testes

```bash
# Todos os testes
dbt test

# Testes de um modelo específico
dbt test --select fogo_cruzado.ocorrencias

# Apenas testes de schema
dbt test --schema
```

### Deploy

O projeto possui **dois ambientes**: **staging** e **prod**.

**Processo automático:**
1. Detecção do prefixo `staging/` na branch ou merge para a branch `main`
2. Build da imagem Docker
3. Push para Google Container Registry
4. Registro dos flows no Prefect
5. Deploy no ambiente correto (staging ou main)


---

## 📚 Recursos Adicionais

- [Documentação Prefect](https://docs-v1.prefect.io/)
- [Documentação DBT](https://docs.getdbt.com/)
- [Biblioteca prefeitura-rio](https://github.com/prefeitura-rio/prefeitura-rio)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)

---
