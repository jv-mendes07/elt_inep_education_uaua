-- Grão: (nível_agregação x unidade_geográfica x localização x dependência x
-- ano x etapa). Só município confirmado até agora (ver
-- stg_distorcao_idade_serie.sql) — sem benchmark Brasil/UF ainda.

select
    cast(null as bigint) as co_entidade,
    co_municipio,
    nivel_agregacao,
    unidade_geografica,
    localizacao,
    dependencia,
    ano,
    etapa_codigo,
    pc_distorcao
from {{ ref('stg_distorcao_idade_serie') }}
