{{ config(materialized='ds_and_dim_creator',
          business_key='businessentityid',
		change_key='lastname,emailaddress' 
        )
}} 
  SELECT load_customer.businessentityid  businessentityid
         , load_customer.title  title
         , load_customer.firstname  firstname
         , load_customer.middlename  middlename
         , load_customer.lastname  lastname
         , load_customer.suffix  suffix
         , load_customer.phonenumber  phonenumber
         , load_customer.phonenumbertype  phonenumbertype
         , load_customer.emailaddress  emailaddress
         , load_customer.emailpromotion  emailpromotion
         , load_customer.addresstype  addresstype
         , load_customer.addressline1  addressline1
         , load_customer.addressline2  addressline2
         , load_customer.city  city
         , load_customer.stateprovincename  stateprovincename
         , load_customer.postalcode  postalcode
         , load_customer.countryregionname  countryregionname
    FROM {{ source('DBT_STG','load_customer')}} load_customer
    WHERE load_customer.addresstype = 'Home'
    
