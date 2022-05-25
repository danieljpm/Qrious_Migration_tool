SELECT
    ds_salesorderdetail.salesorderid,
    ds_salesorderdetail.salesorderdetailid,
    ds_salesorderdetail.carriertrackingnumber,
    ds_salesorderdetail.orderqty,
    ds_salesorderdetail.productid,
    ds_salesorderdetail.specialofferid,
    ds_salesorderdetail.unitprice,
    ds_salesorderdetail.unitpricediscount,
    ds_salesorderdetail.linetotal,
    ds_salesorderdetail.rowguid,
    ds_salesorderdetail.modifieddate
  FROM {{ ref('ds_salesorderdetail')}} ds_salesorderdetail
 
