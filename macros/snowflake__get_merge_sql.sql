{% macro snowflake__get_merge_sql(target, source, unique_key, dest_columns) -%}
    {%- set dest_cols_csv = dest_columns | map(attribute="name") | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE

    {% if unique_key %}
        on DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
    {% else %}
        on FALSE
    {% endif %}

    {% if unique_key %}
    when matched then update set
        {% for column in dest_columns -%}
            {{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}

    {#-- this is new for Snowflake. Always include a `when matched` clause,
      --    but make it a no-op if no unique_key is specified #}
    {% else %}
    when matched then update set
      DBT_INTERNAL_DEST.{{ dest_columns[0].name }} = DBT_INTERNAL_DEST.{{ dest_columns[0].name }}

    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}