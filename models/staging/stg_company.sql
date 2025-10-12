with x as (
  select company_name, company_id, country_code, currency_code
  from {{ source('raw_ref','example_xref_company') }}
)
select company_id, company_name, country_code, currency_code from x
