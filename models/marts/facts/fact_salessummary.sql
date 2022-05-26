{{ config(materialized='table' , 
	 pre_hook="{{pre_fact_operations('DELETE FROM  ' ~ this ~ ' 
    WHERE fact_salessummary.salesorderid IN ( SELECT DISTINCT stage_salessummary.salesorderid FROM  ' ~  ref('stage_salessummary') ~ '  stage_salessummary )')}}"
)}}
SELECT
      dim_order_date_key
    , salesorderid
    , orderdate
    , subtotal
    , taxamt
    , freight
    , totaldue
    , orderqty
    , linetotal
    , GETDATE() as dss_create_time
    , GETDATE() as dss_update_time
    FROM   {{ ref('stage_salessummary')}}
    
    
  
