-- Confirmed against tx_rend_brasil_regioes_ufs_2024.xlsx — Brasil/Região/UF
-- grain only, no CO_MUNICIPIO. Used as BRASIL/REGIAO/UF benchmark rows in
-- fct_rendimento (unioned there with stg_taxas_rendimento_municipios, which
-- is what actually brings Uauá in).

with unpivoted as (
    {{ unpivot_taxas_rendimento_uf(source('raw', 'taxas_rendimento_uf')) }}
),

com_nivel as (
    select
        *,
        case
            when unidade_geografica = 'Brasil' then 'BRASIL'
            when unidade_geografica in (
                'Norte', 'Nordeste', 'Sudeste', 'Sul', 'Centro-Oeste'
            ) then 'REGIAO'
            else 'UF'
        end as nivel_agregacao
    from unpivoted
)

select *
from com_nivel
where pc_aprovacao is not null or pc_reprovacao is not null or pc_abandono is not null
