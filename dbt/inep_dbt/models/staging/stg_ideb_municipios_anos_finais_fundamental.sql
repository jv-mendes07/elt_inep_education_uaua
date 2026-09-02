-- Mesmo padrão de stg_ideb_municipios_anos_iniciais_fundamental.sql,
-- modalidade Anos Finais do Fundamental (6º-9º ano).

{{ unpivot_ideb_municipios(source('raw', 'ideb_municipios_anos_finais_fundamental')) }}
