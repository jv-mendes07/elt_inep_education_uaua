-- Seleção de municípios pares: 100% programática, sem lista hardcoded.
-- Universo candidato = municípios da Região Geográfica Intermediária de
-- Juazeiro (seed_municipios_regiao_alvo, já restringido em stg_escolas).
-- Similaridade socioeconômica é aproximada por dois eixos normalizados:
--   1. distância relativa de população (|pop - pop_uaua| / pop_uaua)
--   2. distância absoluta do percentual de escolas rurais
-- somados com peso igual. Os {{ var('qt_municipios_pares') }} municípios não
-- pertencentes ao alvo com menor distância combinada são marcados is_peer.
--
-- Num backfill multi-ano stg_escolas/stg_populacao trazem vários anos; a
-- seleção de pares precisa de UM snapshot (população e mix rural mudam ano
-- a ano), fixado em var('ano_referencia_pares'). Os fatos e relatórios
-- seguem multi-ano — só esta dimensão é single-year.

{% set ano_ref = var('ano_referencia_pares') %}

with municipios_regiao as (
    select distinct
        co_municipio,
        no_municipio,
        sg_uf,
        co_uf,
        co_regiao_geog_interm
    from {{ ref('stg_escolas') }}
    where nu_ano_censo = {{ ano_ref }}
),

localizacao_mix as (
    select
        co_municipio,
        count(*) as qt_escolas_total,
        count(*) filter (where tp_localizacao = 2)::numeric
            / nullif(count(*), 0) as pc_rural
    from {{ ref('stg_escolas') }}
    where nu_ano_censo = {{ ano_ref }}
    group by co_municipio
),

-- População do ano de referência, ou a estimativa disponível mais recente
-- antes dele (a SIDRA 6579 não tem 2022/2023) — uma linha por município.
populacao_ref as (
    select distinct on (co_municipio)
        co_municipio,
        populacao_estimada,
        ano_referencia as ano_populacao
    from {{ ref('stg_populacao_municipios') }}
    where ano_referencia <= {{ ano_ref }}
    order by co_municipio, ano_referencia desc
),

base as (
    select
        m.co_municipio,
        m.no_municipio,
        m.sg_uf,
        m.co_uf,
        m.co_regiao_geog_interm,
        p.populacao_estimada,
        p.ano_populacao,
        l.pc_rural,
        l.qt_escolas_total,
        (m.co_municipio = {{ var('co_municipio_alvo') }}) as is_target
    from municipios_regiao m
    left join localizacao_mix l on l.co_municipio = m.co_municipio
    left join populacao_ref p on p.co_municipio = m.co_municipio
),

alvo as (
    select populacao_estimada, pc_rural
    from base
    where is_target
),

scored as (
    select
        b.*,
        abs(b.populacao_estimada - a.populacao_estimada)
            / nullif(a.populacao_estimada, 0) as dist_populacao,
        abs(b.pc_rural - a.pc_rural) as dist_rural
    from base b
    cross join alvo a
),

similarity as (
    select
        *,
        coalesce(dist_populacao, 1) + coalesce(dist_rural, 1) as similarity_distance
    from scored
),

ranked as (
    select
        *,
        case when not is_target then row_number() over (
            partition by is_target order by similarity_distance
        ) end as peer_rank
    from similarity
)

select
    co_municipio,
    no_municipio,
    sg_uf,
    co_uf,
    co_regiao_geog_interm,
    {{ ano_ref }} as ano_referencia_pares,
    populacao_estimada,
    ano_populacao,
    pc_rural,
    qt_escolas_total,
    is_target,
    (not is_target and peer_rank <= {{ var('qt_municipios_pares') }}) as is_peer,
    peer_rank
from ranked
order by is_target desc, similarity_distance
