version: 2
sources:
- name: DBT_STG
  database: DW_SALES_ANALYTICS
  schema: DBT_STG
  tables:
  - name: load_customer
    description: Table containing data for load_customer
