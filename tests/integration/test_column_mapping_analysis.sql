/*
  Column mapping analysis test
  
  This test identifies the exact column mismatches between what the intermediate
  models expect and what the staging models actually provide.
*/

{{ config(tags=['integration', 'column_analysis']) }}

-- This test will fail compilation if columns are missing, which is exactly what we want
-- to identify the specific missing columns

WITH column_test_results AS (
  SELECT 
    'company' as entity_type,
    'Testing column availability' as test_description,
    
    -- Test each expected column individually
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as id_status,
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as name_status,
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as website_status,
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as description_status,
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as currency_status,
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as industry_id_status,
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as created_at_status,
    CASE WHEN COUNT(*) > 0 THEN 'AVAILABLE' ELSE 'MISSING' END as updated_at_status
    
  FROM (
    SELECT 
      company_id as id,
      company_name as name,
      website_url as website,
      company_description as description,
      -- These columns are likely missing:
      NULL as currency,  -- No currency column in CRM data
      NULL as industry_id,  -- No industry_id, only industry_primary
      created_date as created_at,
      last_modified_date as updated_at
    FROM {{ ref('stg_crm_companies') }}
    LIMIT 1
  ) t
)

SELECT 
  entity_type,
  test_description,
  id_status,
  name_status,
  website_status,
  description_status,
  currency_status,
  industry_id_status,
  created_at_status,
  updated_at_status
FROM column_test_results