{{ snowflake__get_merge_sql('ds_customer', source('DBT_STG','load_customer'), 'businessentityid', 'title ,firstname ,middlename ,lastname ,suffix ,phonenumber ,phonenumbertype ,emailaddress ,emailpromotion ,addresstype ,addressline1 ,addressline2 ,city ,stateprovincename ,postalcode ,countryregionname')