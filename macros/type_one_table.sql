{% materialization type_one_table, adapter='snowflake' %}
    {%- set business_key = config.get('business_key') -%}
    {%- set unique_key = config.get('unique_key') -%}
    {%- set target_relation = this %} 
    
    {%- set tmp_identifier = "temp_" ~ target_relation.identifier %}
    {%- set tmp_relation = target_relation.incorporate(path=    {"identifier": tmp_identifier, "schema": config.get('temp_schema', default=target_relation.schema)}) -%}
    {%- set existing_relation = load_relation(this) -%}
    {% if existing_relation is none or should_full_refresh() %}
        {%- set build_sql =   build_type_one_table_initial_sql(target_relation, tmp_relation) %}
    {% else %}
        {%- set build_sql = build_type_one_table_sql(target_relation, tmp_relation, business_key, unique_key) %}
    {% endif %}
{{- run_hooks(pre_hooks) -}}
    {%- call statement('main') -%}
        {{ build_sql }}
    {% endcall %}
    {{ run_hooks(post_hooks) }}
    {% set target_relation = this.incorporate(type='table') %}
    {% do persist_docs(target_relation, model) %}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}