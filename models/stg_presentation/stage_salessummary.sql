SELECT
    stage_salesorder.dim_order_date_key,
    stage_salesorder.salesorderid,
    stage_salesorder.orderdate,
    SUM(stage_salesorder.subtotal) subtotal,
    SUM(stage_salesorder.taxamt) taxamt,
    SUM(stage_salesorder.freight) freight,
    SUM(stage_salesorder.totaldue) totaldue,
    SUM(stage_salesorder.orderqty) orderqty,
    SUM(stage_salesorder.linetotal) linetotal
  FROM {{ ref('stage_salesorder')}} stage_salesorder
  GROUP BY stage_salesorder.salesorderid, stage_salesorder.orderdate, stage_salesorder.dim_order_date_key
 
