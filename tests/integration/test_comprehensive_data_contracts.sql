/*
  Comprehensive data contract validation tests for all entity types
  
  These tests validate that all intermediate models meet the complete set of
  data contract requirements including field presence, format validation,
  business rule compliance, and foreign key relationship integrity.
*/

{{ config(tags=['integration', 'comprehensive_contracts']) }}

-- Test 1: Required field presence validation for all entities
WITH required_fields_validation AS (
  -- Company entity required fields
  SELECT 
    'company' as entity_type,
    'required_fields' as validation_category,
    'All required fields must be present' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_company') }}
  
  UNION ALL
  
  -- Fund entity required fields
  SELECT 
    'fund' as entity_type,
    'required_fields' as validation_category,
    'All required fields must be present' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NULL OR name IS NULL OR vintage IS NULL 
        OR base_currency_code IS NULL OR created_at IS NULL OR updated_at IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_fund') }}
  
  UNION ALL
  
  -- Investor entity required fields
  SELECT 
    'investor' as entity_type,
    'required_fields' as validation_category,
    'All required fields must be present' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NULL OR name IS NULL OR investor_type_id IS NULL 
        OR created_at IS NULL OR updated_at IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_investor') }}
  
  UNION ALL
  
  -- Counterparty entity required fields
  SELECT 
    'counterparty' as entity_type,
    'required_fields' as validation_category,
    'All required fields must be present' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NULL OR name IS NULL OR type IS NULL 
        OR created_at IS NULL OR updated_at IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_counterparty') }}
),

-- Test 2: Data format validation for all entities
format_validation AS (
  -- UUID format validation
  SELECT 
    'all_entities' as entity_type,
    'uuid_format' as validation_category,
    'All ID fields must be valid UUIDs' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NOT NULL 
        AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
      THEN 1 ELSE 0 
    END) as violation_count
  FROM (
    SELECT id FROM {{ ref('int_entities_company') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_fund') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_investor') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_counterparty') }}
  )
  
  UNION ALL
  
  -- Currency code format validation
  SELECT 
    'currency_fields' as entity_type,
    'currency_format' as validation_category,
    'Currency codes must be 3-character uppercase' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN currency_code IS NOT NULL 
        AND (LENGTH(currency_code) != 3 OR currency_code != UPPER(currency_code))
      THEN 1 ELSE 0 
    END) as violation_count
  FROM (
    SELECT currency as currency_code FROM {{ ref('int_entities_company') }}
    UNION ALL
    SELECT base_currency_code as currency_code FROM {{ ref('int_entities_fund') }}
  )
  WHERE currency_code IS NOT NULL
  
  UNION ALL
  
  -- Country code format validation
  SELECT 
    'country_fields' as entity_type,
    'country_format' as validation_category,
    'Country codes must be 2-character uppercase' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN country_code IS NOT NULL 
        AND (LENGTH(country_code) != 2 OR country_code != UPPER(country_code))
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_counterparty') }}
  WHERE country_code IS NOT NULL
),

-- Test 3: Business rule compliance validation
business_rules_validation AS (
  -- Fund financial constraints
  SELECT 
    'fund' as entity_type,
    'financial_constraints' as validation_category,
    'Financial fields must meet business constraints' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN (management_fee IS NOT NULL AND management_fee < 0)
        OR (hurdle IS NOT NULL AND hurdle < 0)
        OR (carried_interest IS NOT NULL AND carried_interest < 0)
        OR (target_commitment IS NOT NULL AND target_commitment <= 0)
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_fund') }}
  
  UNION ALL
  
  -- Fund percentage constraints
  SELECT 
    'fund' as entity_type,
    'percentage_constraints' as validation_category,
    'Percentage fields must be between 0 and 1' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN (management_fee IS NOT NULL AND (management_fee < 0 OR management_fee > 1))
        OR (hurdle IS NOT NULL AND (hurdle < 0 OR hurdle > 1))
        OR (carried_interest IS NOT NULL AND (carried_interest < 0 OR carried_interest > 1))
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_fund') }}
  
  UNION ALL
  
  -- Vintage year validation
  SELECT 
    'fund' as entity_type,
    'vintage_validation' as validation_category,
    'Vintage year must be reasonable (1900-current year + 5)' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN vintage IS NOT NULL 
        AND (vintage < 1900 OR vintage > EXTRACT(YEAR FROM CURRENT_DATE()) + 5)
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_fund') }}
  
  UNION ALL
  
  -- Name length validation
  SELECT 
    'all_entities' as entity_type,
    'name_length' as validation_category,
    'Entity names must be reasonable length (1-255 characters)' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN name IS NOT NULL AND (LENGTH(name) = 0 OR LENGTH(name) > 255)
      THEN 1 ELSE 0 
    END) as violation_count
  FROM (
    SELECT name FROM {{ ref('int_entities_company') }}
    UNION ALL
    SELECT name FROM {{ ref('int_entities_fund') }}
    UNION ALL
    SELECT name FROM {{ ref('int_entities_investor') }}
    UNION ALL
    SELECT name FROM {{ ref('int_entities_counterparty') }}
  )
),

-- Test 4: Foreign key relationship integrity
foreign_key_validation AS (
  -- Company industry_id references
  SELECT 
    'company' as entity_type,
    'industry_reference' as validation_category,
    'Company industry_id must reference valid industry' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN c.industry_id IS NOT NULL AND i.id IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_company') }} c
  LEFT JOIN {{ ref('stg_ref_industries') }} i ON c.industry_id = i.id
  WHERE c.industry_id IS NOT NULL
  
  UNION ALL
  
  -- Investor investor_type_id references
  SELECT 
    'investor' as entity_type,
    'investor_type_reference' as validation_category,
    'Investor investor_type_id must reference valid investor type' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN inv.investor_type_id IS NOT NULL AND it.id IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_investor') }} inv
  LEFT JOIN {{ ref('stg_ref_investor_types') }} it ON inv.investor_type_id = it.id
  WHERE inv.investor_type_id IS NOT NULL
  
  UNION ALL
  
  -- Fund base_currency_code references
  SELECT 
    'fund' as entity_type,
    'currency_reference' as validation_category,
    'Fund base_currency_code must reference valid currency' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN f.base_currency_code IS NOT NULL AND c.currency_code IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_fund') }} f
  LEFT JOIN {{ ref('stg_ref_currencies') }} c ON f.base_currency_code = c.currency_code
  WHERE f.base_currency_code IS NOT NULL
  
  UNION ALL
  
  -- Counterparty country_code references
  SELECT 
    'counterparty' as entity_type,
    'country_reference' as validation_category,
    'Counterparty country_code must reference valid country' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN cp.country_code IS NOT NULL AND c.country_code IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_counterparty') }} cp
  LEFT JOIN {{ ref('stg_ref_countries') }} c ON cp.country_code = c.country_code
  WHERE cp.country_code IS NOT NULL
),

-- Test 5: Cross-reference mapping integrity
xref_mapping_validation AS (
  -- Company cross-reference integrity
  SELECT 
    'company' as entity_type,
    'xref_mapping' as validation_category,
    'All companies must have cross-reference mapping' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN x.canonical_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_company') }} c
  LEFT JOIN {{ ref('stg_ref_xref_companies') }} x ON c.id = x.canonical_company_id
  
  UNION ALL
  
  -- Fund cross-reference integrity
  SELECT 
    'fund' as entity_type,
    'xref_mapping' as validation_category,
    'All funds must have cross-reference mapping' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN x.canonical_fund_id IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_fund') }} f
  LEFT JOIN {{ ref('stg_ref_xref_funds') }} x ON f.id = x.canonical_fund_id
  
  UNION ALL
  
  -- Investor cross-reference integrity
  SELECT 
    'investor' as entity_type,
    'xref_mapping' as validation_category,
    'All investors must have cross-reference mapping' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN x.canonical_investor_id IS NULL 
      THEN 1 ELSE 0 
    END) as violation_count
  FROM {{ ref('int_entities_investor') }} i
  LEFT JOIN {{ ref('stg_ref_xref_investors') }} x ON i.id = x.canonical_investor_id
),

-- Test 6: Temporal data validation
temporal_validation AS (
  -- Created/Updated timestamp validation
  SELECT 
    'all_entities' as entity_type,
    'timestamp_validation' as validation_category,
    'Created and updated timestamps must be valid' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN created_at IS NOT NULL AND updated_at IS NOT NULL 
        AND created_at > updated_at
      THEN 1 ELSE 0 
    END) as violation_count
  FROM (
    SELECT created_at, updated_at FROM {{ ref('int_entities_company') }}
    UNION ALL
    SELECT created_at, updated_at FROM {{ ref('int_entities_fund') }}
    UNION ALL
    SELECT created_at, updated_at FROM {{ ref('int_entities_investor') }}
    UNION ALL
    SELECT created_at, updated_at FROM {{ ref('int_entities_counterparty') }}
  )
  
  UNION ALL
  
  -- Future date validation
  SELECT 
    'all_entities' as entity_type,
    'future_date_validation' as validation_category,
    'Timestamps should not be in the far future' as test_description,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN (created_at IS NOT NULL AND created_at > CURRENT_TIMESTAMP() + INTERVAL 1 DAY)
        OR (updated_at IS NOT NULL AND updated_at > CURRENT_TIMESTAMP() + INTERVAL 1 DAY)
      THEN 1 ELSE 0 
    END) as violation_count
  FROM (
    SELECT created_at, updated_at FROM {{ ref('int_entities_company') }}
    UNION ALL
    SELECT created_at, updated_at FROM {{ ref('int_entities_fund') }}
    UNION ALL
    SELECT created_at, updated_at FROM {{ ref('int_entities_investor') }}
    UNION ALL
    SELECT created_at, updated_at FROM {{ ref('int_entities_counterparty') }}
  )
)

-- Combine all validation results and show only failures
SELECT 
  entity_type,
  validation_category,
  test_description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', violation_count, ' violations out of ', total_records, ' records')
  END as test_result,
  total_records,
  violation_count
FROM required_fields_validation
WHERE violation_count > 0

UNION ALL

SELECT 
  entity_type,
  validation_category,
  test_description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', violation_count, ' format violations out of ', total_records, ' records')
  END as test_result,
  total_records,
  violation_count
FROM format_validation
WHERE violation_count > 0

UNION ALL

SELECT 
  entity_type,
  validation_category,
  test_description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', violation_count, ' business rule violations out of ', total_records, ' records')
  END as test_result,
  total_records,
  violation_count
FROM business_rules_validation
WHERE violation_count > 0

UNION ALL

SELECT 
  entity_type,
  validation_category,
  test_description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', violation_count, ' broken foreign key references out of ', total_records, ' records')
  END as test_result,
  total_records,
  violation_count
FROM foreign_key_validation
WHERE violation_count > 0

UNION ALL

SELECT 
  entity_type,
  validation_category,
  test_description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', violation_count, ' missing cross-reference mappings out of ', total_records, ' records')
  END as test_result,
  total_records,
  violation_count
FROM xref_mapping_validation
WHERE violation_count > 0

UNION ALL

SELECT 
  entity_type,
  validation_category,
  test_description,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', violation_count, ' temporal validation failures out of ', total_records, ' records')
  END as test_result,
  total_records,
  violation_count
FROM temporal_validation
WHERE violation_count > 0

ORDER BY entity_type, validation_category