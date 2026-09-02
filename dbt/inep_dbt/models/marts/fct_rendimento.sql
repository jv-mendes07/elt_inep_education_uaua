-- Grão: (nível_agregação x unidade_geográfica x localização x dependência x
-- ano x etapa). União das duas fontes confirmadas: Brasil/Região/UF
-- (benchmark) e Município (traz Uauá e os municípios pares).
-- co_entidade fica nulo até uma fonte no grão escola ser incorporada.

select
    cast(null as bigint) as co_entidade,
    co_municipio,
    nivel_agregacao,
    unidade_geografica,
    localizacao,
    dependencia,
    ano,
    etapa_codigo,
    pc_aprovacao,
    pc_reprovacao,
    pc_abandono
from {{ ref('stg_taxas_rendimento_uf') }}

union all

select
    cast(null as bigint) as co_entidade,
    co_municipio,
    nivel_agregacao,
    unidade_geografica,
    localizacao,
    dependencia,
    ano,
    etapa_codigo,
    pc_aprovacao,
    pc_reprovacao,
    pc_abandono
from {{ ref('stg_taxas_rendimento_municipios') }}
