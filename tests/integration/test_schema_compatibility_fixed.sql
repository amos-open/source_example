/*
  Schema compatibility test after fixes
  
  This test validates that the intermediate models now produce the exact
  columns expected by amos_core canonical models.
*/

{{ config(tags=['integration', 'schema_fixed']) }}

-- Test 1: Company entity schema compatibility
WITH company_test AS (
  SELECT 
    'company' as entity_type,
    COUNT(*) as record_count,
    'Testing required columns exist' as test_description
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
    WHERE id IS NOT NULL
      AND name IS NOT NULL
      AND LENGTH(currency) = 3
      AND created_at IS NOT NULL
      AND updated_at IS NOT NULL
    LIMIT 1
  ) t
),

-- Test 2: Fund entity schema compatibility
fund_test AS (
  SELECT 
    'fund' as entity_type,
    COUNT(*) as record_count,
    'Testing required columns exist' as test_description
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
    WHERE id IS NOT NULL
      AND name IS NOT NULL
      AND LENGTH(base_currency_code) = 3
      AND created_at IS NOT NULL
      AND updated_at IS NOT NULL
    LIMIT 1
  ) t
),

-- Test 3: Investor entity schema compatibility
investor_test AS (
  SELECT 
    'investor' as entity_type,
    COUNT(*) as record_count,
    'Testing required columns exist' as test_description
  FROM (
    SELECT 
      id,
      name,
      investor_type_id,
      created_at,
      updated_at
    FROM {{ ref('int_entities_investor') }}
    WHERE id IS NOT NULL
      AND name IS NOT NULL
      AND investor_type_id IS NOT NULL
      AND created_at IS NOT NULL
      AND updated_at IS NOT NULL
    LIMIT 1
  ) t
),

-- Test 4: Counterparty entity schema compatibility
counterparty_test AS (
  SELECT 
    'counterparty' as entity_type,
    COUNT(*) as record_count,
    'Testing required columns exist' as test_description
  FROM (
    SELECT 
      id,
      name,
      type,
      country_code,
      created_at,
      updated_at
    FROM {{ ref('int_entities_counterparty') }}
    WHERE id IS NOT NULL
      AND name IS NOT NULL
      AND created_at IS NOT NULL
      AND updated_at IS NOT NULL
    LIMIT 1
  ) t
)

SELECT 
  entity_type,
  test_description,
  record_count,
  CASE 
    WHEN record_count > 0 THEN 'PASS'
    ELSE 'FAIL'
  END as test_result
FROM company_test

UNION ALL

SELECT 
  entity_type,
  test_description,
  record_count,
  CASE 
    WHEN record_count > 0 THEN 'PASS'
    ELSE 'FAIL'
  END as test_result
FROM fund_test

UNION ALL

SELECT 
  entity_type,
  test_description,
  record_count,
  CASE 
    WHEN record_count > 0 THEN 'PASS'
    ELSE 'FAIL'
  END as test_result
FROM investor_test

UNION ALL

SELECT 
  entity_type,
  test_description,
  record_count,
  CASE 
    WHEN record_count > 0 THEN 'PASS'
    ELSE 'FAIL'
  END as test_result
FROM counterparty_test

ORDER BY entity_type