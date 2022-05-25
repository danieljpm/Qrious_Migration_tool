{{ config(materialized='ds_and_dim_creator',
          business_key='salesorderid,salesorderdetailid' 
        )
}} 
  SELECT load_salesorderdetail.salesorderid  salesorderid
         , load_salesorderdetail.salesorderdetailid  salesorderdetailid
         , load_salesorderdetail.carriertrackingnumber  carriertrackingnumber
         , load_salesorderdetail.orderqty  orderqty
         , load_salesorderdetail.productid  productid
         , load_salesorderdetail.specialofferid  specialofferid
         , load_salesorderdetail.unitprice  unitprice
         , load_salesorderdetail.unitpricediscount  unitpricediscount
         , load_salesorderdetail.linetotal  linetotal
         , load_salesorderdetail.rowguid  rowguid
         , load_salesorderdetail.modifieddate  modifieddate
    FROM {{ source('DBT_STG','load_salesorderdetail')}} load_salesorderdetail
    
