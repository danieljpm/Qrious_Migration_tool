SELECT
ds_customer.businessentityid,
ds_customer.title,
ds_customer.firstname,
ds_customer.middlename,
ds_customer.lastname,
ds_customer.suffix,
ds_customer.phonenumber,
ds_customer.phonenumbertype,
ds_customer.emailaddress,
ds_customer.emailpromotion,
ds_customer.addresstype,
ds_customer.addressline1,
ds_customer.addressline2,
ds_customer.city,
ds_customer.stateprovincename,
ds_customer.postalcode,
ds_customer.countryregionname
FROM {{ ref('ds_customer')}} ds_customer
WHERE ds_customer.dss_current_flag = 'Y'
