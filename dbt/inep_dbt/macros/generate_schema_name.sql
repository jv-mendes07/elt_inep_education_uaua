{#
    dbt's default generate_schema_name macro concatenates the profile's target
    schema with the model's custom schema (e.g. "public_staging"). This
    project wants clean, predictable schema names instead — "staging" and
    "marts" — consistent with the "raw" schema already created by the
    Airflow-loaded tables. See dbt_project.yml for the +schema config per
    model folder.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
