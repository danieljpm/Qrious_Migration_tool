{%- macro ds_and_dim_initial_create_sql(target_relation, temp_relation, business_key, change_key) -%}
    {% set distant_past  ="to_timestamp_ltz('1900-01-01 00:00:00.000')"%}
    {% set distant_future ="to_timestamp_ltz('2999-12-31 23:59:59.000')"%}
    {{ create_table_as(True, temp_relation, sql) }}
    {% set CurrentTimeStamp ="to_timestamp_ltz(current_timestamp())"%}
    {%- set business_key_list = business_key.split(',') -%}
    {%- set change_key_list = change_key.split(',') -%}
    
    {%- set initial_sql -%}
    {% if change_key != 'unknown'  %}
     -- Type 2 solution
      SELECT 
        {{ dbt_utils.surrogate_key(business_key_list + change_key_list ) }} as {{target_relation.name}}_KEY 
        ,*
        ,{{ distant_past }} as DSS_START_DATE
        ,{{ distant_future }} as DSS_END_DATE
        , 'Y' as DSS_CURRENT_FLAG
        ,{{ CurrentTimeStamp }} as DSS_CREATE_DATE
        ,{{ CurrentTimeStamp }} as DSS_UPDATE_DATE
      FROM
        {{ temp_relation }}
   
    {%else%}
    -- Type 1 solution
      SELECT
        {{ dbt_utils.surrogate_key(business_key_list) }} as {{target_relation.name}}_KEY 
        , *
        ,{{ CurrentTimeStamp }} as DSS_CREATE_DATE
        ,{{ CurrentTimeStamp }} as DSS_UPDATE_DATE
      FROM
        {{ temp_relation }}
    {%endif%}
    {%- endset -%}
    {{ create_table_as(False, target_relation, initial_sql) }}
{%- endmacro -%}
 
