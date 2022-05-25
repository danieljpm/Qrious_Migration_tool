{{ config(materialized='ds_and_dim_creator',
          business_key='productid' 
        )
}} 
  SELECT stage_product.productid  productid
         , stage_product.name  name
         , stage_product.productnumber  productnumber
         , stage_product.makeflag  makeflag
         , stage_product.finishedgoodsflag  finishedgoodsflag
         , stage_product.color  color
         , stage_product.safetystocklevel  safetystocklevel
         , stage_product.reorderpoint  reorderpoint
         , stage_product.standardcost  standardcost
         , stage_product.listprice  listprice
         , stage_product.size  size
         , stage_product.sizeunitmeasurecode  sizeunitmeasurecode
         , stage_product.weightunitmeasurecode  weightunitmeasurecode
         , stage_product.weight  weight
         , stage_product.daystomanufacture  daystomanufacture
         , stage_product.productline  productline
         , stage_product.class  class
         , stage_product.style  style
         , stage_product.productsubcategoryid  productsubcategoryid
         , stage_product.productmodelid  productmodelid
         , stage_product.sellstartdate  sellstartdate
         , stage_product.sellenddate  sellenddate
         , stage_product.discontinueddate  discontinueddate
         , stage_product.rowguid  rowguid
         , stage_product.modifieddate  modifieddate
    FROM {{ ref('stage_product')}} stage_product
    
