SELECT
    ds_product.productid,
    ds_product.name,
    ds_product.productnumber,
    ds_product.makeflag,
    ds_product.finishedgoodsflag,
    ds_product.color,
    ds_product.safetystocklevel,
    ds_product.reorderpoint,
    ds_product.standardcost,
    ds_product.listprice,
    ds_product.size,
    ds_product.sizeunitmeasurecode,
    ds_product.weightunitmeasurecode,
    ds_product.weight,
    ds_product.daystomanufacture,
    ds_product.productline,
    ds_product.class,
    ds_product.style,
    ds_product.productsubcategoryid,
    ds_product.productmodelid,
    ds_product.sellstartdate,
    ds_product.sellenddate,
    ds_product.discontinueddate,
    ds_product.rowguid,
    ds_product.modifieddate
  FROM {{ ref('ds_product')}} ds_product
 
