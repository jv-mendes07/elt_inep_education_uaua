-- Confirmed against divulgacao_{brasil,ufs}_Ensino_Medio_integrado_ideb_2025.xlsx.
-- Brasil/UF level, modalidade ensino médio integrado only — used as
-- benchmark rows in fct_ideb (unioned there with the município staging
-- models, which is what actually brings Uauá in).
--
-- Two separate raw tables (not one shared table): running the DAG revealed
-- "ufs" has an extra 2017 edition "brasil" doesn't, so their column counts
-- differ and a shared raw table breaks COPY — see config.ideb_raw_table
-- and docs/data_sources.md. Harmless here since both still carry the
-- 2019/2021/2023/2025 columns this macro actually unpivots.

{{ unpivot_ideb_brasil_uf(source('raw', 'ideb_brasil_ensino_medio_integrado')) }}

union all

{{ unpivot_ideb_brasil_uf(source('raw', 'ideb_ufs_ensino_medio_integrado')) }}
