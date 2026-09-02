# Fontes de dados — pipeline INEP Uauá/BA

Este documento é a fonte única de verdade para as URLs de download usadas pelo
DAG `inep_etl_dag`. O INEP não oferece uma API versionada e estável para estes
conjuntos de dados — as páginas de download são renderizadas via JavaScript e
não puderam ser resolvidas automaticamente via ferramentas de fetch
automatizado (confirmado em 2026-08-29). As URLs abaixo foram confirmadas
manualmente em um navegador em 2026-09-01 e já estão registradas como
default nas funções `get_*_url()` de `dags/inep_pipeline/config.py` — não é
necessário configurar Airflow Variables para usá-las hoje; as Variables
existem apenas como *override* para quando uma URL mudar (ano seguinte, por
exemplo) sem precisar alterar código:

```bash
docker compose exec airflow-webserver airflow variables set <nome_da_variable> "<nova_url>"
```

**Nota sobre backfill multi-ano**: as URLs seguem um padrão previsível
(`.../<ano>/<arquivo>_<ano>.zip`), o que tornaria um backfill automático por
template de URL bem simples de implementar — deliberadamente **adiado até o
pipeline de um único ano (2024) estar validado ponta a ponta**, por decisão
do projeto.

## Status por fonte

Todas as 6 URLs abaixo foram baixadas, descompactadas e tiveram as colunas
reais confirmadas em 2026-09-01 (inspeção via XML da planilha, já que
pandas/openpyxl não estão disponíveis no ambiente local de desenvolvimento).
**Uauá (CO_MUNICIPIO=2932002) foi localizado e confirmado presente** nos
3 arquivos de nível Município.

| Indicador | Nível | Airflow Variable | Arquivo local | Status |
|---|---|---|---|---|
| Taxas de Rendimento Escolar 2024 | Brasil/Região/UF | `inep_taxas_rendimento_uf_url` | `notebooks/data/taxas_rendimento_2024/` | ✅ URL registrada, colunas confirmadas. Usado como benchmark. |
| Taxas de Rendimento Escolar 2024 | **Município** | `inep_taxas_rendimento_municipios_url` | `notebooks/data/taxas_rendimento_2024_municipios/` | ✅ URL registrada, colunas confirmadas, **Uauá presente**. |
| Taxas de Distorção Idade-série 2024 | **Município** | `inep_distorcao_municipios_url` | `notebooks/data/distorcao_idade_serie_2024_municipios/` | ✅ URL registrada, colunas confirmadas, **Uauá presente**. ⚠️ Sem equivalente Brasil/UF ainda (ver nota abaixo). |
| IDEB, modalidade Ensino Médio Integrado | Brasil | `inep_ideb_url_brasil_ensino_medio_integrado` | `notebooks/data/ideb_2019_2025/brasil_ensino_medio_integrado/` | ✅ URL registrada, colunas confirmadas. Usado como benchmark. |
| IDEB, modalidade Ensino Médio Integrado | UF | `inep_ideb_url_ufs_ensino_medio_integrado` | `notebooks/data/ideb_2019_2025/ufs_ensino_medio_integrado/` | ✅ idem. |
| IDEB, modalidade Anos Iniciais do Fundamental | **Município** | `inep_ideb_url_municipios_anos_iniciais_fundamental` | `notebooks/data/ideb_2019_2025/municipios_anos_iniciais_fundamental/` | ✅ URL registrada, colunas confirmadas, **Uauá presente**. |
| IDEB, modalidade Anos Finais do Fundamental | **Município** | `inep_ideb_url_municipios_anos_finais_fundamental` | `notebooks/data/ideb_2019_2025/municipios_anos_finais_fundamental/` | ✅ URL registrada, colunas assumidas idênticas ao anos_iniciais (não individualmente inspecionadas — mesmo template). |
| IDEB, modalidade Ensino Médio (regular) | **Município** | `inep_ideb_url_municipios_ensino_medio` | `notebooks/data/ideb_2019_2025/municipios_ensino_medio/` | ✅ idem. |
| IDEB por escola | Escola | `inep_ideb_url_escolas_*` (a definir) | _pendente_ | ⚠️ Ver nota "IDEB por escola" abaixo — não incluído no v1. |
| Taxas de rendimento/distorção por escola | Escola | — | _pendente_ | ⚠️ Fora do escopo do v1 (grão atual: Município). |
| População dos municípios | Brasil (API) | `ibge_populacao_sidra_url` | n/a (API) | ✅ API pública, JSON, sem autenticação. |

### ⚠️ Nota: distorção idade-série sem benchmark Brasil/UF

Só o arquivo nível Município foi confirmado para distorção idade-série — o
INEP também publica um nível Brasil/UF equivalente (mesmo padrão de taxas de
rendimento), mas ainda não foi baixado. `fct_distorcao`/`rpt_distorcao`
funcionam sem ele (só não têm linha de comparação nacional/estadual); baixar
e adicionar segue o mesmo padrão de `stg_taxas_rendimento_uf.sql` quando
for necessário.

### ⚠️ Nota importante sobre IDEB por escola

Diferente das taxas de rendimento/distorção (que publicam planilhas em todos
os níveis, inclusive Escola), a consulta de resultados do IDEB **por
escola** migrou para um painel de BI interativo no portal do INEP, navegado
manualmente por UF → Município → Escola, sem um arquivo de download em massa
aparente. O grão Município (já confirmado e integrado) é suficiente para o
v1 do dashboard; per-escola fica como melhoria futura. Se for necessário:

- **(a)** procurar se ainda existe uma planilha agregada por escola em
  `download.inep.gov.br/ideb/...`;
- **(b)** inspecionar as chamadas de rede do painel de BI em busca de um
  endpoint de dados subjacente;
- **(c)** na ausência de (a)/(b), exportar manualmente do painel e carregar
  como arquivo local em `notebooks/data/ideb_2019_2025/escolas_<modalidade>/`
  — `extract_ideb` já procura um arquivo local ali antes de tentar baixar.

## Estrutura de colunas confirmada

Todos os arquivos INEP (`.xlsx`) enterram o cabeçalho de código
machine-readable sob várias linhas de título/cabeçalho mesclado — confirmado
que a linha varia por família de arquivo mas é **consistente dentro da
família**, mesmo entre Brasil/UF/Município (ver `*_HEADER_ROW` em
`dags/inep_pipeline/config.py`):
- Taxas de rendimento e distorção idade-série: linha 9 da planilha (`header=8`).
- IDEB (todas as modalidades/níveis): linha 10 da planilha (`header=9`).

`'-'` e `'--'` são os dois placeholders que o INEP usa para "sem dado" —
tratados como NULL em todos os macros de unpivot
(`dbt/inep_dbt/macros/unpivot_*.sql`).

- **Censo Escolar 2024** (`notebooks/data/microdados_censo_escolar_2024_defeso/dados/microdados_ed_basica_2024.csv`):
  ver `dbt/inep_dbt/models/staging/stg_escolas.sql`. Dicionário oficial em
  `notebooks/data/microdados_censo_escolar_2024_defeso/Anexos/ANEXO I - Dicionário de Dados/`.

- **Taxas de Rendimento** (`tx_rend_brasil_regioes_ufs_2024.xlsx` /
  `tx_rend_municipios_2024.xlsx`): identificação
  `NU_ANO_CENSO, UNIDGEO, NO_CATEGORIA (localização), NO_DEPENDENCIA` (Brasil/
  UF) ou `NU_ANO_CENSO, NO_REGIAO, SG_UF, CO_MUNICIPIO, NO_MUNICIPIO,
  NO_CATEGORIA, NO_DEPENDENCIA` (Município); depois 54 colunas de taxa
  `{1|2|3}_CAT_{etapa}` — prefixo `1`=aprovação/`2`=reprovação/`3`=abandono;
  etapa `FUN`/`FUN_AI`/`FUN_AF`/`FUN_01..09`/`MED`/`MED_01..04`/`MED_NS`.
  Unpivotado em `dbt/inep_dbt/macros/unpivot_taxas_rendimento.sql` (só os
  agregados FUN_AI/FUN_AF/MED — detalhe por série fica fora do v1).

- **Taxa de Distorção Idade-série** (`TDI_MUNICIPIOS_2024.xlsx`): mesma
  identificação do arquivo de município acima; colunas de taxa
  `{etapa}_CAT_0` (um indicador só, sem prefixo 1/2/3) — etapa
  `FUN`/`FUN_AI`/`FUN_AF`/`FUN_01..09`/`MED`/`MED_01..04`. Unpivotado em
  `dbt/inep_dbt/macros/unpivot_distorcao.sql`.

- **IDEB** (`divulgacao_{brasil,ufs}_Ensino_Medio_integrado_ideb_2025.xlsx` /
  `divulgacao_{anos_iniciais,anos_finais,ensino_medio}_municipios_2025.xlsx`):
  identificação `UNIDADE_GEOGRAFICA`/`REDE` (Brasil/UF — colunas sem código
  na fonte, renomeadas por posição em `dags/inep_pipeline/extract.py`) ou
  `SG_UF`/`CO_MUNICIPIO`/`NO_MUNICIPIO`/`REDE` (Município — já vêm com
  código). Depois, por edição: `VL_APROVACAO_<ano>_*`,
  `VL_INDICADOR_REND_<ano>` (componente P), `VL_NOTA_MATEMATICA/PORTUGUES/
  MEDIA_<ano>` (Saeb), e **`VL_OBSERVADO_<ano>` = a nota IDEB** (escala
  0-10). Os arquivos de município trazem edições de 2005 a 2025; só
  2019/2021/2023/2025 são desempilhados (escopo do projeto). Unpivotado em
  `dbt/inep_dbt/macros/unpivot_ideb.sql`.

  **Cada (nível, modalidade) tem sua própria tabela `raw`** — inicialmente
  Brasil e UF pareciam compartilháveis (mesmas colunas), mas rodar o DAG de
  ponta a ponta revelou que o arquivo UF tem uma edição 2017 extra que o
  arquivo Brasil não tem, então o número de colunas diverge e uma tabela
  raw compartilhada quebra o `COPY`. Ver `config.ideb_raw_table` — a regra
  agora é simplesmente uma tabela por fonte, sem exceção.

## Convenção de pasta local / fallback manual

`dags/inep_pipeline/extract.py` procura um arquivo local em
`notebooks/data/<pasta>/` (qualquer nome, `.csv`/`.xlsx`/`.xls`/`.ods`)
antes de tentar baixar da web — é assim que os 6 arquivos acima foram
confirmados e integrados sem depender do Airflow rodando. Útil para testar
uma fonte nova rapidamente: baixe manualmente, solte em
`notebooks/data/<nome_da_pasta>/`, e rode a extração — sem precisar
registrar Variable nenhuma até estar pronto para automatizar.

_Última verificação: 2026-09-01._
