{{ config(materialized='ds_and_dim_creator',
          business_key='businessentityid' 
        )
}} 
  SELECT stage_customer.businessentityid  businessentityid
         , stage_customer.title  title
         , stage_customer.firstname  firstname
         , stage_customer.middlename  middlename
         , stage_customer.lastname  lastname
         , stage_customer.suffix  suffix
         , stage_customer.phonenumber  phonenumber
         , stage_customer.phonenumbertype  phonenumbertype
         , stage_customer.emailaddress  emailaddress
         , stage_customer.emailpromotion  emailpromotion
         , stage_customer.addresstype  addresstype
         , stage_customer.addressline1  addressline1
         , stage_customer.addressline2  addressline2
         , stage_customer.city  city
         , stage_customer.stateprovincename  stateprovincename
         , stage_customer.postalcode  postalcode
         , stage_customer.countryregionname  countryregionname
    FROM {{ ref('stage_customer')}} stage_customer
    
