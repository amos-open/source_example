select
  opportunity_id,
  fund_code,
  opportunity_name as name,
  stage_name,
  stage_type,
  expected_amount,
  close_date,
  company_name,
  company_domain,
  company_country_code,
  industry_name,
  responsible,
  source_system,
  created_at,
  updated_at
from {{ source('raw_example_crm','example_crm_opportunities') }}
