/*
  Missing columns detection test
  
  This test identifies which columns are missing from the intermediate models
  that are required by the amos_core canonical models.
*/

{{ config(tags=['integration', 'missing_columns']) }}

-- Test by attempting to select the exact columns expected by amos_core
WITH company_column_test AS (
  SELECT 
    'company' as model_name,
    'Attempting to select amos_core expected columns' as test_description,
    COUNT(*) as record_count
  FROM (
    SELECT 
      id,
      name,
      website,
      description,
      currency,
      industry_id,
      created_at,
      updated_at
    FROM {{ ref('int_entities_company') }}
    LIMIT 1
  ) t
),

fund_column_test AS (
  SELECT 
    'fund' as model_name,
    'Attempting to select amos_core expected columns' as test_description,
    COUNT(*) as record_count
  FROM (
    SELECT 
      id,
      name,
      type,
      vintage,
      management_fee,
      hurdle,
      carried_interest,
      target_commitment,
      incorporated_in,
      base_currency_code,
      created_at,
      updated_at
    FROM {{ ref('int_entities_fund') }}
    LIMIT 1
  ) t
),

investor_column_test AS (
  SELECT 
    'investor' as model_name,
    'Attempting to select amos_core expected columns' as test_description,
    COUNT(*) as record_count
  FROM (
    SELECT 
      id,
      name,
      investor_type_id,
      created_at,
      updated_at
    FROM {{ ref('int_entities_investor') }}
    LIMIT 1
  ) t
),

counterparty_column_test AS (
  SELECT 
    'counterparty' as model_name,
    'Attempting to select amos_core expected columns' as test_description,
    COUNT(*) as record_count
  FROM (
    SELECT 
      id,
      name,
      type,
      country_code,
      created_at,
      updated_at
    FROM {{ ref('int_entities_counterparty') }}
    LIMIT 1
  ) t
)

SELECT * FROM company_column_test
UNION ALL
SELECT * FROM fund_column_test  
UNION ALL
SELECT * FROM investor_column_test
UNION ALL
SELECT * FROM counterparty_column_test