{%- macro ds_and_dim_update_sql(target_relation, temp_relation, business_key, change_key) -%}
-- comma seperated string of table columns
{%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}
-- List of table columns
{%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
-- List of Business Keys
{%- set business_key_list = business_key.split(',') -%}
-- List of unique Keys
{%- if change_key !='unknown' %}
-- if list not empty split string
-- Type 2
{%- set change_key_list = change_key.split(',') -%}
{%- else -%}
-- Type 1
-- keep list empty
{% set change_key_list = change_key  -%}
{%- endif -%}
{%- set CurrentTimeStamp ="to_timestamp_ltz(current_timestamp())" -%}
{%-set distant_future ="to_timestamp_ltz('2999-12-31 23:59:59.000')" -%}
-- Generate temp table for source data
{{ create_table_as(True, temp_relation, sql) }}
merge into {{ target_relation }} as DBT_INTERNAL_DEST
using (
select
{% for column in dest_columns if column.name not in 'DSS_START_DATE','DSS_END_DATE','DSS_CREATE_DATE','DSS_UPDATE_DATE','DSS_CURRENT_FLAG'-%}
tmp.{{ column.name }}
{% if not loop.last  %}, {%- endif %}
{%- endfor %}
from {{temp_relation}} as tmp
EXCEPT
select
{% for column in dest_columns if column.name not in 'DSS_START_DATE','DSS_END_DATE','DSS_CREATE_DATE','DSS_UPDATE_DATE','DSS_CURRENT_FLAG'-%}
trg.{{ column.name }}
{% if not loop.last  %}, {%- endif %}
{%- endfor %}
from {{target_relation}} as trg
{%if change_key != 'unknown' -%}
where trg.DSS_CURRENT_FLAG = 'Y'
{%- endif -%}
) as DBT_INTERNAL_SOURCE
-- create join cluase on business keys
{% if business_key -%}
on
{% for bk_item in business_key_list -%}
( DBT_INTERNAL_DEST.{{ bk_item  }} = DBT_INTERNAL_SOURCE.{{ bk_item }}
)
{%- if not loop.last  %} and  {%- endif %}
{%- endfor %}
{% else -%}
on FALSE
{% endif -%}
{% if business_key and change_key !='unknown' -%}
-- Type 2
when matched and (DBT_INTERNAL_DEST.DSS_CURRENT_FLAG = 'Y') and
(
-- add clause to compare unique list
{% for uk_item in change_key_list -%}
( DBT_INTERNAL_DEST.{{ uk_item  }} != DBT_INTERNAL_SOURCE.{{ uk_item }}
OR ( DBT_INTERNAL_DEST.{{ uk_item  }} IS     NULL AND DBT_INTERNAL_SOURCE.{{ uk_item }} IS NOT NULL )
OR ( DBT_INTERNAL_DEST.{{ uk_item  }} IS NOT NULL AND DBT_INTERNAL_SOURCE.{{ uk_item }} IS     NULL )
)
{%- if not loop.last  %} or  {%- endif %}
{%- endfor %}
)
then update set
-- Age record
DSS_END_DATE = dateadd(second, -1, {{ CurrentTimeStamp }})
,DSS_CURRENT_FLAG = 'N'
,DSS_UPDATE_DATE = {{ CurrentTimeStamp }}
when matched and (DBT_INTERNAL_DEST.DSS_CURRENT_FLAG = 'Y')
then update set
{% for column in dest_columns if ((column.name not in business_key_list) and (column.name not in change_key_list) and (column.name not in 'DSS_START_DATE','DSS_END_DATE','DSS_CREATE_DATE','DSS_CURRENT_FLAG')) -%}
{% if column.name == 'DSS_UPDATE_DATE' -%}
{{ column.name }} = {{ CurrentTimeStamp }}
{% else -%}
{{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }}
{% endif -%}
{%- if not loop.last  %}, {%- endif %}
{%- endfor %}
{% else -%}
-- Type 1
when matched then update set
-- dont update Business Keys and dont update when record was first written
{% for column in dest_columns if ((column.name not in business_key_list) and (column.name not in change_key_list) and (column.name != 'DSS_CREATE_DATE')) -%}
{% if column.name == 'DSS_UPDATE_DATE' -%}
{{ column.name }} = {{ CurrentTimeStamp }}
{% else -%}
{{ column.name }} = DBT_INTERNAL_SOURCE.{{ column.name }}
{% endif -%}
{%- if not loop.last  %}, {%- endif %}
{%- endfor %}
{% endif %}
when not matched then insert
({{ dest_cols_csv }})
values
(
{% for column in dest_columns -%}
{% if column.name in 'DSS_START_DATE','DSS_CREATE_DATE','DSS_UPDATE_DATE' -%}
{{ CurrentTimeStamp }}
{% elif column.name == 'DSS_END_DATE' -%}
{{ distant_future }}
{% elif column.name == 'DSS_CURRENT_FLAG' -%}
'Y'
{% else -%}
DBT_INTERNAL_SOURCE.{{ column.name }}
{% endif -%}
{%- if not loop.last  %}, {%- endif %}
{%- endfor %}
)
;
{% if change_key!='unknown' -%}
insert into {{target_relation}}
select {{temp_relation}}.*
,{{ CurrentTimeStamp }}
,{{ distant_future }}
,'Y'
,{{ CurrentTimeStamp }}
,{{ CurrentTimeStamp }}
from {{temp_relation}}
EXCEPT
select
{% for column in dest_columns if column.name not in 'DSS_START_DATE','DSS_END_DATE','DSS_CREATE_DATE','DSS_UPDATE_DATE','DSS_CURRENT_FLAG'-%}
{{target_relation}}.{{ column.name }}
{% if not loop.last  %}, {%- endif %}
{%- endfor %}
,{{ CurrentTimeStamp }}
,{{ distant_future }}
,'Y'
,{{ CurrentTimeStamp }}
,{{ CurrentTimeStamp }}
from {{target_relation}}
where {{target_relation}}.DSS_CURRENT_FLAG = 'Y'
{% endif %}
{%- endmacro -%}
