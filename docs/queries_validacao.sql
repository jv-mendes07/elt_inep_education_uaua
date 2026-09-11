-- Consultas de validação do pipeline INEP Uauá/BA.
-- Rodar depois de um DAG run:
--   docker compose exec postgres psql -U postgres -d inep -f /opt/airflow/docs/queries_validacao.sql
-- Foco no backfill multi-ano: cada bloco imprime a cobertura de anos por camada.

\echo '=== raw: tabelas e contagem por ano ==='
select 'escolas' as tabela, "NU_ANO_CENSO" as ano, count(*)
from raw.escolas group by 1,2
union all select 'taxas_rendimento_uf', "NU_ANO_CENSO", count(*)
from raw.taxas_rendimento_uf group by 1,2
union all select 'taxas_rendimento_municipios', "NU_ANO_CENSO", count(*)
from raw.taxas_rendimento_municipios group by 1,2
union all select 'distorcao_idade_serie_municipios', "NU_ANO_CENSO", count(*)
from raw.distorcao_idade_serie_municipios group by 1,2
order by 1,2;

\echo ''
\echo '=== raw.populacao_municipios: anos (D3C) presentes ==='
select "D3C" as ano, count(*) from raw.populacao_municipios group by 1 order by 1;

\echo ''
\echo '=== raw.escolas: escolas de Uauá por ano (2024 deve ser 38) ==='
select "NU_ANO_CENSO" as ano, count(*) as escolas_uaua
from raw.escolas where "CO_MUNICIPIO" = '2932002' group by 1 order by 1;

\echo ''
\echo '=== staging: cobertura de anos ==='
select 'stg_escolas' as modelo, nu_ano_censo as ano, count(*) from staging.stg_escolas group by 1,2
union all select 'stg_taxas_rendimento_municipios', ano, count(*) from staging.stg_taxas_rendimento_municipios group by 1,2
union all select 'stg_distorcao_idade_serie', ano, count(*) from staging.stg_distorcao_idade_serie group by 1,2
union all select 'stg_populacao_municipios', ano_referencia, count(*) from staging.stg_populacao_municipios group by 1,2
order by 1,2;

\echo ''
\echo '=== marts.dim_municipio: 18 linhas, 1 target, 8 peers, populacao preenchida ==='
select co_municipio, no_municipio, ano_referencia_pares, populacao_estimada,
       ano_populacao, round(pc_rural::numeric,3) as pc_rural, is_target, is_peer, peer_rank
from marts.dim_municipio order by is_target desc, peer_rank nulls last;

\echo ''
\echo '=== marts.dim_ano: anos x tipo_indicador ==='
select tipo_indicador, array_agg(ano order by ano) as anos
from marts.dim_ano group by 1 order by 1;

\echo ''
\echo '=== marts: cobertura de anos nos fatos ==='
select 'fct_matriculas' as fato, ano, count(*) from marts.fct_matriculas group by 1,2
union all select 'fct_rendimento', ano, count(*) from marts.fct_rendimento group by 1,2
union all select 'fct_distorcao', ano, count(*) from marts.fct_distorcao group by 1,2
union all select 'fct_ideb', ano, count(*) from marts.fct_ideb group by 1,2
order by 1,2;

\echo ''
\echo '=== marts.reporting: cobertura de anos (o que o Superset vai enxergar) ==='
select 'rpt_matriculas' as rpt, ano, count(*) from marts.rpt_matriculas group by 1,2
union all select 'rpt_rendimento', ano, count(*) from marts.rpt_rendimento group by 1,2
union all select 'rpt_distorcao', ano, count(*) from marts.rpt_distorcao group by 1,2
union all select 'rpt_ideb', ano, count(*) from marts.rpt_ideb group by 1,2
order by 1,2;

\echo ''
\echo '=== rpt_rendimento: série histórica de Uauá (abandono no ensino médio) ==='
select ano, etapa_codigo, round(pc_aprovacao,1) ap, round(pc_reprovacao,1) rp, round(pc_abandono,1) ab
from marts.rpt_rendimento
where is_target and nivel_agregacao = 'MUNICIPIO' and etapa_codigo = 'med'
order by ano;
