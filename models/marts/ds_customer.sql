{{ config(materialized='append_only',
          unique_key='BUSINESSENTITYID') }}
  {% set distant_past       ="TO_DATE('1900-01-01')"%}
  {% set distant_future     ="TO_DATE('2999-12-31')"%}
  SELECT
  *
,current_timestamp() as dss_create_time
,current_timestamp() as dss_update_time
  FROM
    {{source('DBT_STG','load_customer')}}
    WHERE addresstype = 'Home' 