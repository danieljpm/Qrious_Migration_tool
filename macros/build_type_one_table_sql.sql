{%- macro build_type_one_table_sql(target_relation, temp_relation,business_key,unique_key) -%}
    {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) %}
    
    {{ create_table_as(True, temp_relation, sql) }}

    merge into {{ target_relation }} as DBT_INTERNAL_DEST
    using {{ temp_relation }} as DBT_INTERNAL_SOURCE

    {% if business_key %}
        on DBT_INTERNAL_SOURCE.{{ business_key }} = DBT_INTERNAL_DEST.{{ business_key }}
    {% else %}
        on FALSE
    {% endif %}

    {% if business_key %}
    when matched then update set
        {% for column in dest_columns if ((column.name != business_key) and (column.name != 'DSS_CREATE_TIME')) -%}
            {{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }}
            {%- if not loop.last  %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{%- endmacro -%}