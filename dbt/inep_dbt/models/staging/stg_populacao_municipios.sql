-- Fonte: API SIDRA/IBGE, tabela 6579 (estimativas de população residente),
-- variável 9324, nível territorial n6 (município) — ver docs/data_sources.md.
-- A extração (dags/inep_pipeline/extract.py) preserva as chaves originais do
-- JSON da SIDRA sem renomear, então raw.populacao_municipios chega com as
-- colunas padrão da API: D1C/D1N (código/nome do município), D2C/D2N ou
-- D3C/D3N (variável e período, a ordem exata depende de como a tabela 6579
-- declara suas classificações) e V (valor).
--
-- TODO(verificar): confirmar contra uma chamada real da API se o período é
-- D2 ou D3 antes de rodar em produção — ajustar a coluna referenciada abaixo
-- se necessário. Mantido como pass-through mínimo até essa confirmação.

select
    "D1C"::numeric::int as co_municipio,
    "D1N" as no_municipio,
    "V"::numeric as populacao_estimada
from {{ source('raw', 'populacao_municipios') }}
where "D1C" is not null
  and "V" not in ('...', '-', '')  -- SIDRA usa placeholders textuais para valores ausentes
