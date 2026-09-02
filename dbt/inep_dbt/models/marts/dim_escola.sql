select distinct
    co_entidade,
    no_entidade,
    tp_dependencia,
    tp_localizacao,
    co_municipio,
    nu_ano_censo as ano_censo
from {{ ref('stg_escolas') }}
