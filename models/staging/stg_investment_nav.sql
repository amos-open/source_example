select
  fund_code,
  company_name,
  as_of_date,
  current_nav,
  currency_code,
  'EXAMPLE' as source_system
from {{ source('raw_example_admin','example_fund_admin_investment_nav') }}
