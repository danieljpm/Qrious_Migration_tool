{{ config(materialized='ds_and_dim_creator',
          business_key='productid' 
        )
}} 
  SELECT load_product.productid  productid
         , load_product.name  name
         , load_product.productnumber  productnumber
         , load_product.makeflag  makeflag
         , load_product.finishedgoodsflag  finishedgoodsflag
         , load_product.color  color
         , load_product.safetystocklevel  safetystocklevel
         , load_product.reorderpoint  reorderpoint
         , load_product.standardcost  standardcost
         , load_product.listprice  listprice
         , load_product.size  size
         , load_product.sizeunitmeasurecode  sizeunitmeasurecode
         , load_product.weightunitmeasurecode  weightunitmeasurecode
         , load_product.weight  weight
         , load_product.daystomanufacture  daystomanufacture
         , load_product.productline  productline
         , load_product.class  class
         , load_product.style  style
         , load_product.productsubcategoryid  productsubcategoryid
         , load_product.productmodelid  productmodelid
         , load_product.sellstartdate  sellstartdate
         , load_product.sellenddate  sellenddate
         , load_product.discontinueddate  discontinueddate
         , load_product.rowguid  rowguid
         , load_product.modifieddate  modifieddate
    FROM {{ source('DBT_STG','load_product')}} load_product
    
