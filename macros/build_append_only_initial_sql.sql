{%- macro build_append_only_initial_sql(target_relation, temp_relation) -%}
    {{ create_table_as(True, temp_relation, sql) }}
    {% set distant_past       ="TO_DATE('1900-01-01')"%}
    {% set distant_future     ="TO_DATE('2999-12-31')"%}
    {%- set initial_sql -%}
        SELECT
          *
        FROM
          {{ temp_relation }}
    {%- endset -%}
    {{ create_table_as(False, target_relation, initial_sql) }}
{%- endmacro -%}