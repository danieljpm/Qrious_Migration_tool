{{ config(materialized='ds_and_dim_creator',
          business_key='businessentityid',
		change_key='jobtitle' 
        )
}} 
  SELECT stage_employee.businessentityid  businessentityid
         , stage_employee.title  title
         , stage_employee.firstname  firstname
         , stage_employee.middlename  middlename
         , stage_employee.lastname  lastname
         , stage_employee.suffix  suffix
         , stage_employee.jobtitle  jobtitle
         , stage_employee.phonenumber  phonenumber
         , stage_employee.phonenumbertype  phonenumbertype
         , stage_employee.emailaddress  emailaddress
         , stage_employee.emailpromotion  emailpromotion
         , stage_employee.addressline1  addressline1
         , stage_employee.addressline2  addressline2
         , stage_employee.city  city
         , stage_employee.stateprovincename  stateprovincename
         , stage_employee.postalcode  postalcode
         , stage_employee.countryregionname  countryregionname
    FROM {{ ref('stage_employee')}} stage_employee
    
