-- Mesmo padrão de stg_ideb_municipios_anos_iniciais_fundamental.sql,
-- modalidade Ensino Médio regular (não "integrado").

{{ unpivot_ideb_municipios(source('raw', 'ideb_municipios_ensino_medio')) }}
