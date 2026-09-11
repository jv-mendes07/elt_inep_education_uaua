-- Fonte: API SIDRA/IBGE, tabela 6579 (estimativas de população residente),
-- variável 9324, nível territorial n6 (município) — ver docs/data_sources.md.
-- A extração (dags/inep_pipeline/extract.py) preserva as chaves originais do
-- JSON da SIDRA sem renomear, então raw.populacao_municipios chega com as
-- colunas padrão da API: D1C/D1N (código/nome do município), D2C/D2N
-- (variável), D3C/D3N (ano) e V (valor).
--
-- Confirmado contra uma chamada real (2026-09-09): o período/ano é a
-- classificação D3 (D3C = "Ano (Código)"). Uma linha por (município, ano).
-- A tabela 6579 cobre 2001-2021 e 2024+ para o município-alvo (sem 2022/2023,
-- anos de Censo) — quem consome escolhe o ano de referência
-- (marts/dim_municipio.sql pega o ano disponível mais próximo <= o alvo).

select
    "D1C"::numeric::int as co_municipio,
    "D1N" as no_municipio,
    "D3C"::numeric::int as ano_referencia,
    "V"::numeric as populacao_estimada
from {{ source('raw', 'populacao_municipios') }}
where "D1C" is not null
  and "V" not in ('...', '-', '')  -- SIDRA usa placeholders textuais para valores ausentes
