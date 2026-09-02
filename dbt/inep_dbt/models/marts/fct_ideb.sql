-- Grão: (nível_agregação x modalidade x unidade_geográfica x rede x edição).
-- União das 4 fontes confirmadas: Brasil/UF (benchmark, ensino médio
-- integrado) e Município x 3 modalidades padrão (anos iniciais/finais,
-- ensino médio regular) — as que trazem Uauá.

{% set municipios_models = [
    'stg_ideb_municipios_anos_iniciais_fundamental',
    'stg_ideb_municipios_anos_finais_fundamental',
    'stg_ideb_municipios_ensino_medio',
] %}

select
    cast(null as bigint) as co_entidade,
    co_municipio,
    nivel_agregacao,
    modalidade,
    unidade_geografica,
    rede,
    ano_edicao as ano,
    nota_ideb,
    indicador_rendimento,
    nota_saeb_media
from {{ ref('stg_ideb_brasil_uf') }}

{% for m in municipios_models %}
union all

select
    cast(null as bigint) as co_entidade,
    co_municipio,
    nivel_agregacao,
    modalidade,
    unidade_geografica,
    rede,
    ano_edicao as ano,
    nota_ideb,
    indicador_rendimento,
    nota_saeb_media
from {{ ref(m) }}
{% endfor %}
