# ELT INEP Educação — Uauá/BA

Pipeline de dados educacionais públicos do INEP/IBGE para o município de
**Uauá - BA** (código IBGE `2932002`), com comparação contra municípios
socioeconomicamente similares na Região Geográfica Intermediária de Juazeiro
e contra médias estadual/nacional.

## Objetivo

Coletar Censo Escolar, taxas de rendimento (aprovação/reprovação/abandono),
distorção idade-série e IDEB; padronizar em séries históricas por escola,
etapa de ensino e ano; e disponibilizar um dashboard analítico no Superset
para apoiar o monitoramento pedagógico da gestão escolar municipal.

## Arquitetura

```
INEP/IBGE (download) → Airflow (extract/load) → Postgres (raw)
                                                     ↓
                                              dbt (staging → marts)
                                                     ↓
                                              Superset (dashboard)
```

- **Postgres 16** — banco `inep`, camadas `raw` → `staging` → `marts`.
- **Apache Airflow 2.9** — orquestra extração, carga e execução do dbt
  (`dags/inep_etl_dag.py`).
- **dbt** — normaliza o formato largo do censo em um grão
  escola × etapa de ensino × ano, e implementa a seleção programática de
  municípios pares (`dbt/inep_dbt/`).
- **Apache Superset** — dashboard "Educação Básica — Uauá/BA".
- **JupyterLab** — ambiente de exploração (`notebooks/`).

Subir tudo localmente:
```bash
docker compose up -d postgres
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler superset jupyter pgadmin
```
Airflow: `localhost:8080` · Superset: `localhost:8088` · pgAdmin: `localhost:5050`
(credenciais em `.env`).

## Status

- ✅ Pipeline validado ponta a ponta (Airflow + Postgres): Censo, taxas de
  rendimento (Brasil/UF + município), distorção idade-série e IDEB
  2019–2025 extraem, carregam em `raw.*` e normalizam via dbt até
  `marts.reporting.rpt_*`. Todos os testes dbt passam.
- ✅ **Backfill multi-ano** — o DAG faz dynamic task mapping sobre a
  Variable `inep_anos_ingestao` (default `[2024]`). Para carregar 2019–2025:
  ```bash
  docker compose exec airflow-webserver airflow variables set inep_anos_ingestao '[2019,2020,2021,2022,2023,2024,2025]'
  docker compose exec airflow-webserver airflow dags trigger inep_etl_dag
  ```
  URLs montadas por template `{ano}` em `config.py`, com override por ano
  via Variable. Ver [`docs/data_sources.md`](docs/data_sources.md).
- ⏳ Dashboard Superset — a construir sobre os modelos `marts.reporting.rpt_*`.

O plano completo de implementação (contexto, decisões e critérios de
conclusão do v1) está em
`C:\Users\João Victor\.claude\plans\i-have-this-data-radiant-newell.md`.

## Estrutura do repositório

```
dags/inep_etl_dag.py       DAG principal (extract → load raw → dbt → data quality)
dags/inep_pipeline/        config.py, extract.py, load.py
dbt/inep_dbt/models/       staging/ (1:1 com raw) e marts/ (dims/facts + reporting/)
docs/data_sources.md       URLs de download e status de cada fonte
notebooks/                 exploração (inep_qedu_data_dev.ipynb) e dados brutos locais
```
