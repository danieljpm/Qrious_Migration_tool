SELECT
    ds_employee.businessentityid,
    ds_employee.title,
    ds_employee.firstname,
    ds_employee.middlename,
    ds_employee.lastname,
    ds_employee.suffix,
    ds_employee.jobtitle,
    ds_employee.phonenumber,
    ds_employee.phonenumbertype,
    ds_employee.emailaddress,
    ds_employee.emailpromotion,
    ds_employee.addressline1,
    ds_employee.addressline2,
    ds_employee.city,
    ds_employee.stateprovincename,
    ds_employee.postalcode,
    ds_employee.countryregionname
  FROM {{ ref('ds_employee')}} ds_employee
 
