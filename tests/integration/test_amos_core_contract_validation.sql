/*
  AMOS Core Contract Validation Test
  
  This test validates that the intermediate models produce data that exactly
  matches the contracts enforced by amos_core canonical models.
*/

{{ config(tags=['integration', 'contract_validation']) }}

-- Test data type compatibility and business rules
WITH validation_results AS (
  
  -- Company validation
  SELECT 
    'company' as entity_type,
    'id_format' as validation_type,
    COUNT(*) as violation_count,
    'UUID format validation for company.id' as description
  FROM {{ ref('int_entities_company') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'company' as entity_type,
    'currency_length' as validation_type,
    COUNT(*) as violation_count,
    'Currency code must be 3 characters' as description
  FROM {{ ref('int_entities_company') }}
  WHERE currency IS NOT NULL AND LENGTH(currency) != 3
  
  UNION ALL
  
  SELECT 
    'company' as entity_type,
    'required_fields' as validation_type,
    COUNT(*) as violation_count,
    'Required fields must not be null' as description
  FROM {{ ref('int_entities_company') }}
  WHERE id IS NULL OR name IS NULL
  
  UNION ALL
  
  -- Fund validation
  SELECT 
    'fund' as entity_type,
    'id_format' as validation_type,
    COUNT(*) as violation_count,
    'UUID format validation for fund.id' as description
  FROM {{ ref('int_entities_fund') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'currency_length' as validation_type,
    COUNT(*) as violation_count,
    'Base currency code must be 3 characters' as description
  FROM {{ ref('int_entities_fund') }}
  WHERE base_currency_code IS NOT NULL AND LENGTH(base_currency_code) != 3
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'business_rules' as validation_type,
    COUNT(*) as violation_count,
    'Management fee must be non-negative' as description
  FROM {{ ref('int_entities_fund') }}
  WHERE management_fee IS NOT NULL AND management_fee < 0
  
  UNION ALL
  
  SELECT 
    'fund' as entity_type,
    'business_rules' as validation_type,
    COUNT(*) as violation_count,
    'Target commitment must be positive' as description
  FROM {{ ref('int_entities_fund') }}
  WHERE target_commitment IS NOT NULL AND target_commitment <= 0
  
  UNION ALL
  
  -- Investor validation
  SELECT 
    'investor' as entity_type,
    'id_format' as validation_type,
    COUNT(*) as violation_count,
    'UUID format validation for investor.id' as description
  FROM {{ ref('int_entities_investor') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'investor' as entity_type,
    'required_fields' as validation_type,
    COUNT(*) as violation_count,
    'Required fields must not be null' as description
  FROM {{ ref('int_entities_investor') }}
  WHERE id IS NULL OR name IS NULL
  
  UNION ALL
  
  -- Counterparty validation
  SELECT 
    'counterparty' as entity_type,
    'id_format' as validation_type,
    COUNT(*) as violation_count,
    'UUID format validation for counterparty.id' as description
  FROM {{ ref('int_entities_counterparty') }}
  WHERE id IS NOT NULL 
    AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
  
  UNION ALL
  
  SELECT 
    'counterparty' as entity_type,
    'country_code_length' as validation_type,
    COUNT(*) as violation_count,
    'Country code must be 2 characters' as description
  FROM {{ ref('int_entities_counterparty') }}
  WHERE country_code IS NOT NULL AND LENGTH(country_code) != 2
  
  UNION ALL
  
  SELECT 
    'counterparty' as entity_type,
    'required_fields' as validation_type,
    COUNT(*) as violation_count,
    'Required fields must not be null' as description
  FROM {{ ref('int_entities_counterparty') }}
  WHERE id IS NULL OR name IS NULL
)

SELECT 
  entity_type,
  validation_type,
  description,
  violation_count,
  CASE 
    WHEN violation_count = 0 THEN 'PASS'
    ELSE 'FAIL'
  END as validation_result
FROM validation_results
WHERE violation_count > 0  -- Only show failures
ORDER BY entity_type, validation_type