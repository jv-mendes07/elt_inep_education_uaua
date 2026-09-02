-- Confirmed against divulgacao_anos_iniciais_municipios_2025.xlsx (Uauá
-- present: CO_MUNICIPIO=2932002, 3 linhas de REDE — Estadual/Municipal/
-- Pública). This is the modalidade that covers most of Uauá's fundamental
-- schools' 1º-5º ano.

{{ unpivot_ideb_municipios(source('raw', 'ideb_municipios_anos_iniciais_fundamental')) }}
