{% materialization ds_and_dim_creator %}
    {%- set business_key = config.get('business_key') -%}
    {%- set change_key = config.get('change_key',default='unknown') -%}
    {%- set target_relation = this %} 
    
    {%- set tmp_identifier = "temp_" ~ target_relation.identifier %}
    {%- set tmp_relation = target_relation.incorporate(path= {"identifier": tmp_identifier, "schema": config.get('temp_schema', default=target_relation.schema)}) -%}
    {%- set existing_relation = load_relation(this) -%}
    {% if existing_relation is none or should_full_refresh() %}
    --  If table does not yet exist, create new table and set initial dss values
        {%- set build_sql = ds_and_dim_initial_create_sql(target_relation, tmp_relation, business_key, change_key) %}
    {% else %}
    -- Table exist, update and age records
        {%- set build_sql = ds_and_dim_update_sql(target_relation, tmp_relation, business_key, change_key) %}
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
