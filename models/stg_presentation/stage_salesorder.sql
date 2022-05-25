SELECT
    IFNULL(dim_customer.dim_customer_key,'0') dim_customer_key,
    IFNULL(dim_employee.dim_employee_key,'0') dim_employee_key,
    IFNULL(dim_product.dim_product_key,'0') dim_product_key,
    IFNULL(dim_order_date.dim_order_date_key,'0') dim_order_date_key,
    stage_salesorderheader.salesorderid,
    stage_salesorderheader.revisionnumber,
    stage_salesorderheader.orderdate,
    stage_salesorderheader.duedate,
    stage_salesorderheader.shipdate,
    stage_salesorderheader.status,
    stage_salesorderheader.onlineorderflag,
    stage_salesorderheader.salesordernumber,
    stage_salesorderheader.purchaseordernumber,
    stage_salesorderheader.accountnumber,
    stage_salesorderheader.customerid,
    stage_salesorderheader.salespersonid,
    stage_salesorderheader.territoryid,
    stage_salesorderheader.billtoaddressid,
    stage_salesorderheader.shiptoaddressid,
    stage_salesorderheader.shipmethodid,
    stage_salesorderheader.creditcardid,
    stage_salesorderheader.creditcardapprovalcode,
    stage_salesorderheader.currencyrateid,
    stage_salesorderheader.subtotal,
    stage_salesorderheader.taxamt,
    stage_salesorderheader.freight,
    stage_salesorderheader.totaldue,
    stage_salesorderheader.comment,
    stage_salesorderheader.rowguid,
    stage_salesorderheader.modifieddate,
    stage_salesorderdetail.salesorderdetailid,
    stage_salesorderdetail.carriertrackingnumber,
    stage_salesorderdetail.orderqty,
    stage_salesorderdetail.productid,
    stage_salesorderdetail.specialofferid,
    stage_salesorderdetail.unitprice,
    stage_salesorderdetail.unitpricediscount,
    stage_salesorderdetail.linetotal
  FROM {{ ref('stage_salesorderheader')}} stage_salesorderheader
  JOIN {{ ref('stage_salesorderdetail')}} stage_salesorderdetail
    ON stage_salesorderheader.salesorderid = stage_salesorderdetail.salesorderid
    LEFT OUTER JOIN {{ ref('dim_customer')}}
    ON stage_salesorderheader.customerid = dim_customer.businessentityid
    LEFT OUTER JOIN {{ ref('dim_employee')}}
    ON stage_salesorderheader.salespersonid = dim_employee.businessentityid
    AND dim_employee.dss_current_flag = @v_dss_current_flag
    LEFT OUTER JOIN {{ ref('dim_product')}}
    ON stage_salesorderdetail.productid = dim_product.productid
    LEFT OUTER JOIN {{ ref('dim_order_date')}}
    ON date_trunc('DAY', stage_salesorderheader.orderdate) = dim_order_date.order_date
 
