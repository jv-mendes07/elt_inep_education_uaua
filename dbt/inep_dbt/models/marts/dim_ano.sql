-- Anos/edições presentes em cada fonte, com um marcador de tipo de
-- indicador. Construído por UNION (em vez de hardcoded) para que um futuro
-- backfill multi-ano seja aditivo — basta a staging trazer mais anos. O
-- select distinct externo deduplica combinações (ano, tipo_indicador) que
-- aparecem em mais de uma fonte do mesmo tipo (ex.: taxa_rendimento vem de
-- UF e Município; ideb vem de 4 fontes diferentes).

select distinct ano, tipo_indicador
from (

    select nu_ano_censo as ano, 'censo_escolar' as tipo_indicador
    from {{ ref('stg_escolas') }}

    union all

    select ano, 'taxa_rendimento' as tipo_indicador
    from {{ ref('stg_taxas_rendimento_uf') }}

    union all

    select ano, 'taxa_rendimento' as tipo_indicador
    from {{ ref('stg_taxas_rendimento_municipios') }}

    union all

    select ano, 'distorcao_idade_serie' as tipo_indicador
    from {{ ref('stg_distorcao_idade_serie') }}

    union all

    select ano_edicao as ano, 'ideb' as tipo_indicador
    from {{ ref('stg_ideb_brasil_uf') }}

    union all

    select ano_edicao as ano, 'ideb' as tipo_indicador
    from {{ ref('stg_ideb_municipios_anos_iniciais_fundamental') }}

    union all

    select ano_edicao as ano, 'ideb' as tipo_indicador
    from {{ ref('stg_ideb_municipios_anos_finais_fundamental') }}

    union all

    select ano_edicao as ano, 'ideb' as tipo_indicador
    from {{ ref('stg_ideb_municipios_ensino_medio') }}

) todas_as_fontes
