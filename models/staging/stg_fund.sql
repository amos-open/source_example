with x as (
  select fund_code, fund_id, fund_name, base_currency_code
  from {{ source('raw_ref','example_xref_fund') }}
)
select
  fund_id,
  fund_code,
  fund_name,
  base_currency_code
from x
