{{ config(materialized='ds_and_dim_creator',
          business_key='salesorderid' 
        )
}} 
  SELECT load_salesorderheader.salesorderid  salesorderid
         , load_salesorderheader.revisionnumber  revisionnumber
         , load_salesorderheader.orderdate  orderdate
         , load_salesorderheader.duedate  duedate
         , load_salesorderheader.shipdate  shipdate
         , load_salesorderheader.status  status
         , load_salesorderheader.onlineorderflag  onlineorderflag
         , load_salesorderheader.salesordernumber  salesordernumber
         , load_salesorderheader.purchaseordernumber  purchaseordernumber
         , load_salesorderheader.accountnumber  accountnumber
         , load_salesorderheader.customerid  customerid
         , load_salesorderheader.salespersonid  salespersonid
         , load_salesorderheader.territoryid  territoryid
         , load_salesorderheader.billtoaddressid  billtoaddressid
         , load_salesorderheader.shiptoaddressid  shiptoaddressid
         , load_salesorderheader.shipmethodid  shipmethodid
         , load_salesorderheader.creditcardid  creditcardid
         , load_salesorderheader.creditcardapprovalcode  creditcardapprovalcode
         , load_salesorderheader.currencyrateid  currencyrateid
         , load_salesorderheader.subtotal  subtotal
         , load_salesorderheader.taxamt  taxamt
         , load_salesorderheader.freight  freight
         , load_salesorderheader.totaldue  totaldue
         , load_salesorderheader.comment  comment
         , load_salesorderheader.rowguid  rowguid
         , load_salesorderheader.modifieddate  modifieddate
    FROM {{ source('DBT_STG','load_salesorderheader')}} load_salesorderheader
    
