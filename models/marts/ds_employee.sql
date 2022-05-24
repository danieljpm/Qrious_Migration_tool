{{ config(materialized='ds_and_dim_creator',
          business_key='businessentityid' 
        )
}} 
  SELECT
  *
  FROM {{source('DBT_STG','load_employee')}}