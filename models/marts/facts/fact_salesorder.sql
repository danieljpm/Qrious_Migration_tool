{{ config(materialized='table',
	pre_hook="{{pre_fact_operations(('TRUNCATE TABLE  ' ~ this ~ '')}}"
)}}
SELECT
      dim_customer_key
    , dim_employee_key
    , dim_product_key
    , dim_order_date_key
    , salesorderid
    , revisionnumber
    , orderdate
    , duedate
    , shipdate
    , status
    , onlineorderflag
    , salesordernumber
    , purchaseordernumber
    , accountnumber
    , customerid
    , salespersonid
    , territoryid
    , billtoaddressid
    , shiptoaddressid
    , shipmethodid
    , creditcardid
    , creditcardapprovalcode
    , currencyrateid
    , subtotal
    , taxamt
    , freight
    , totaldue
    , comment
    , rowguid
    , modifieddate
    , salesorderdetailid
    , carriertrackingnumber
    , orderqty
    , productid
    , specialofferid
    , unitprice
    , unitpricediscount
    , linetotal
    , GETDATE() as dss_create_time
    , GETDATE() as dss_update_time
    FROM   {{ ref('stage_salesorder')}}
    
    
  
