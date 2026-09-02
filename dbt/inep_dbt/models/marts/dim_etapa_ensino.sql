select
    etapa_codigo,
    etapa_nome,
    nivel_agregado,
    ordem::int as ordem
from {{ ref('seed_etapa_ensino_mapping') }}
