{{ config(materialized='type_one_table',
          business_key='BUSINESSENTITYID',
          unique_key= none) }} 
  SELECT
  *
,current_timestamp() as dss_create_time
,current_timestamp() as dss_update_time
  FROM
    {{source('DBT_STG','load_customer')}}
  where addresstype = 'Home'