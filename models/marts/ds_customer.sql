{{ config(materialized='ds_and_dim_creator',
          business_key='BUSINESSENTITYID',
          change_key='firstname,emailaddress' 
        )
}} 
  SELECT
  *
  FROM {{source('DBT_STG','load_customer')}}
  where addresstype = 'Home'