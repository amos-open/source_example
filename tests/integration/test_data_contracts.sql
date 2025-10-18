/*
  Data contract validation tests for amos_core integration
  
  These tests validate that the data produced by intermediate models
  meets the business rules and constraints expected by amos_core.
*/

{{ config(tags=['integration', 'data_contracts']) }}

-- Test 1: Required field validation
WITH required_fields_test AS (
  SELECT 
    'company' as entity_type,
    'Missing required id field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_company') }}
  WHERE id IS NULL
  
  UNION ALL
  
  SELECT 
    'company' as entity_type,
    'Missing required name field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_company') }}
  WHERE name IS NULL
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'Missing required id field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE id IS NULL
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'Missing required name field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE name IS NULL
  
  UNION ALL
  
  SELECT 
    'investor' as entity_type,
    'Missing required id field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_investor') }}
  WHERE id IS NULL
  
  UNION ALL
  
  SELECT 
    'investor' as entity_type,
    'Missing required name field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_investor') }}
  WHERE name IS NULL
  
  UNION ALL
  
  SELECT 
    'counterparty' as entity_type,
    'Missing required id field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_counterparty') }}
  WHERE id IS NULL
  
  UNION ALL
  
  SELECT 
    'counterparty' as entity_type,
    'Missing required name field' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_counterparty') }}
  WHERE name IS NULL
),

-- Test 2: Business rule validation
business_rules_test AS (
  SELECT 
    'fund' as entity_type,
    'Invalid management_fee (negative value)' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE management_fee IS NOT NULL AND management_fee < 0
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'Invalid hurdle (negative value)' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE hurdle IS NOT NULL AND hurdle < 0
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'Invalid carried_interest (negative value)' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE carried_interest IS NOT NULL AND carried_interest < 0
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'Invalid target_commitment (non-positive value)' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE target_commitment IS NOT NULL AND target_commitment <= 0
  
  UNION ALL
  
  SELECT 
    'company' as entity_type,
    'Invalid currency code (not 3 characters)' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_company') }}
  WHERE currency IS NOT NULL AND LENGTH(currency) != 3
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'Invalid base_currency_code (not 3 characters)' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE base_currency_code IS NOT NULL AND LENGTH(base_currency_code) != 3
  
  UNION ALL
  
  SELECT 
    'counterparty' as entity_type,
    'Invalid country_code (not 2 characters)' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_counterparty') }}
  WHERE country_code IS NOT NULL AND LENGTH(country_code) != 2
),

-- Test 3: Data type validation
data_type_test AS (
  SELECT 
    'company' as entity_type,
    'Invalid UUID format for id' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_company') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'company' as entity_type,
    'Invalid UUID format for industry_id' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_company') }}
  WHERE industry_id IS NOT NULL 
    AND NOT REGEXP_LIKE(industry_id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'Invalid UUID format for id' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_fund') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'investor' as entity_type,
    'Invalid UUID format for id' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_investor') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'investor' as entity_type,
    'Invalid UUID format for investor_type_id' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_investor') }}
  WHERE investor_type_id IS NOT NULL 
    AND NOT REGEXP_LIKE(investor_type_id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'counterparty' as entity_type,
    'Invalid UUID format for id' as violation_type,
    COUNT(*) as violation_count
  FROM {{ ref('int_entities_counterparty') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
),

-- Combine all test results
all_violations AS (
  SELECT * FROM required_fields_test WHERE violation_count > 0
  UNION ALL
  SELECT * FROM business_rules_test WHERE violation_count > 0
  UNION ALL
  SELECT * FROM data_type_test WHERE violation_count > 0
)

SELECT 
  entity_type,
  violation_type,
  violation_count,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END as test_status
FROM all_violations
ORDER BY entity_type, violation_type