with x as (
  select investor_code, investor_id, investor_name
  from {{ source('raw_ref','example_xref_investor') }}
)
select investor_id, investor_code, investor_name from x
