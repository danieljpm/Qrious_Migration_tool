{{ config(materialized='ds_and_dim_creator',
          business_key='businessentityid' 
        )
}} 
  SELECT load_employee.businessentityid  businessentityid
         , load_employee.title  title
         , load_employee.firstname  firstname
         , load_employee.middlename  middlename
         , load_employee.lastname  lastname
         , load_employee.suffix  suffix
         , load_employee.jobtitle  jobtitle
         , load_employee.phonenumber  phonenumber
         , load_employee.phonenumbertype  phonenumbertype
         , load_employee.emailaddress  emailaddress
         , load_employee.emailpromotion  emailpromotion
         , load_employee.addressline1  addressline1
         , load_employee.addressline2  addressline2
         , load_employee.city  city
         , load_employee.stateprovincename  stateprovincename
         , load_employee.postalcode  postalcode
         , load_employee.countryregionname  countryregionname
    FROM {{ source('DBT_STG','load_employee')}} load_employee
    
