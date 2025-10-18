/*
  Integration tests to validate compatibility with amos_core canonical models
  
  These tests verify that the intermediate models in amos_source_example
  produce data that matches the exact schema contracts expected by amos_core,
  including enhanced canonical identifier support and updated business rules.
  
  Test Categories:
  1. Schema compatibility - column names and data types with canonical IDs
  2. Data contract validation - required fields and enhanced constraints
  3. Foreign key relationships - reference data integrity with canonical mappings
  4. Business rule validation - enhanced data quality and format checks
  5. Canonical identifier validation - UUID format and cross-reference consistency
*/

-- Test 1: Company entity schema compatibility with canonical identifiers
{{ config(tags=['integration', 'amos_core_compatibility', 'enhanced']) }}

WITH company_schema_test AS (
  SELECT
    -- Test that all required columns exist with correct data types including canonical IDs
    CASE 
      WHEN COUNT(*) = 0 THEN 'PASS'
      ELSE 'FAIL: Missing or incorrect columns in int_entities_company'
    END as test_result,
    
    -- List any missing columns including canonical identifier fields
    ARRAY_TO_STRING(ARRAY[
      CASE WHEN id IS NULL THEN 'id (uuid - canonical)' END,
      CASE WHEN name IS NULL THEN 'name (varchar(255))' END,
      CASE WHEN website IS NULL THEN 'website (varchar(255))' END,
      CASE WHEN description IS NULL THEN 'description (text)' END,
      CASE WHEN currency IS NULL THEN 'currency (varchar(3))' END,
      CASE WHEN industry_id IS NULL THEN 'industry_id (uuid)' END,
      CASE WHEN created_at IS NULL THEN 'created_at (timestamp)' END,
      CASE WHEN updated_at IS NULL THEN 'updated_at (timestamp)' END,
      CASE WHEN crm_company_id IS NULL THEN 'crm_company_id (varchar(50) - source)' END,
      CASE WHEN pm_company_id IS NULL THEN 'pm_company_id (varchar(50) - source)' END
    ], ', ') as missing_columns
    
  FROM (
    SELECT 
      TRY_CAST(id AS VARCHAR(36)) as id,
      TRY_CAST(name AS VARCHAR(255)) as name,
      TRY_CAST(website AS VARCHAR(255)) as website,
      TRY_CAST(description AS TEXT) as description,
      TRY_CAST(currency AS VARCHAR(3)) as currency,
      TRY_CAST(industry_id AS VARCHAR(36)) as industry_id,
      TRY_CAST(created_at AS TIMESTAMP) as created_at,
      TRY_CAST(updated_at AS TIMESTAMP) as updated_at,
      TRY_CAST(crm_company_id AS VARCHAR(50)) as crm_company_id,
      TRY_CAST(pm_company_id AS VARCHAR(50)) as pm_company_id
    FROM {{ ref('int_entities_company') }}
    LIMIT 1
  ) t
  WHERE id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL
)

SELECT * FROM company_schema_test
WHERE test_result = 'FAIL'

UNION ALL

-- Test 2: Fund entity schema compatibility
SELECT
  CASE 
    WHEN COUNT(*) = 0 THEN 'PASS'
    ELSE 'FAIL: Missing or incorrect columns in int_entities_fund'
  END as test_result,
  
  ARRAY_TO_STRING(ARRAY[
    CASE WHEN id IS NULL THEN 'id (uuid)' END,
    CASE WHEN name IS NULL THEN 'name (varchar(255))' END,
    CASE WHEN type IS NULL THEN 'type (varchar(64))' END,
    CASE WHEN vintage IS NULL THEN 'vintage (integer)' END,
    CASE WHEN management_fee IS NULL THEN 'management_fee (decimal(7,4))' END,
    CASE WHEN hurdle IS NULL THEN 'hurdle (decimal(7,4))' END,
    CASE WHEN carried_interest IS NULL THEN 'carried_interest (decimal(7,4))' END,
    CASE WHEN target_commitment IS NULL THEN 'target_commitment (numeric(20,2))' END,
    CASE WHEN incorporated_in IS NULL THEN 'incorporated_in (varchar(128))' END,
    CASE WHEN base_currency_code IS NULL THEN 'base_currency_code (varchar(3))' END,
    CASE WHEN created_at IS NULL THEN 'created_at (timestamp)' END,
    CASE WHEN updated_at IS NULL THEN 'updated_at (timestamp)' END
  ], ', ') as missing_columns

FROM (
  SELECT 
    TRY_CAST(id AS VARCHAR(36)) as id,
    TRY_CAST(name AS VARCHAR(255)) as name,
    TRY_CAST(type AS VARCHAR(64)) as type,
    TRY_CAST(vintage AS INTEGER) as vintage,
    TRY_CAST(management_fee AS DECIMAL(7,4)) as management_fee,
    TRY_CAST(hurdle AS DECIMAL(7,4)) as hurdle,
    TRY_CAST(carried_interest AS DECIMAL(7,4)) as carried_interest,
    TRY_CAST(target_commitment AS NUMERIC(20,2)) as target_commitment,
    TRY_CAST(incorporated_in AS VARCHAR(128)) as incorporated_in,
    TRY_CAST(base_currency_code AS VARCHAR(3)) as base_currency_code,
    TRY_CAST(created_at AS TIMESTAMP) as created_at,
    TRY_CAST(updated_at AS TIMESTAMP) as updated_at
  FROM {{ ref('int_entities_fund') }}
  LIMIT 1
) t
WHERE id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL

UNION ALL

-- Test 3: Investor entity schema compatibility  
SELECT
  CASE 
    WHEN COUNT(*) = 0 THEN 'PASS'
    ELSE 'FAIL: Missing or incorrect columns in int_entities_investor'
  END as test_result,
  
  ARRAY_TO_STRING(ARRAY[
    CASE WHEN id IS NULL THEN 'id (uuid)' END,
    CASE WHEN name IS NULL THEN 'name (varchar(255))' END,
    CASE WHEN investor_type_id IS NULL THEN 'investor_type_id (uuid)' END,
    CASE WHEN created_at IS NULL THEN 'created_at (timestamp)' END,
    CASE WHEN updated_at IS NULL THEN 'updated_at (timestamp)' END
  ], ', ') as missing_columns

FROM (
  SELECT 
    TRY_CAST(id AS VARCHAR(36)) as id,
    TRY_CAST(name AS VARCHAR(255)) as name,
    TRY_CAST(investor_type_id AS VARCHAR(36)) as investor_type_id,
    TRY_CAST(created_at AS TIMESTAMP) as created_at,
    TRY_CAST(updated_at AS TIMESTAMP) as updated_at
  FROM {{ ref('int_entities_investor') }}
  LIMIT 1
) t
WHERE id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL

UNION ALL

-- Test 4: Counterparty entity schema compatibility
SELECT
  CASE 
    WHEN COUNT(*) = 0 THEN 'PASS'
    ELSE 'FAIL: Missing or incorrect columns in int_entities_counterparty'
  END as test_result,
  
  ARRAY_TO_STRING(ARRAY[
    CASE WHEN id IS NULL THEN 'id (uuid)' END,
    CASE WHEN name IS NULL THEN 'name (varchar(255))' END,
    CASE WHEN type IS NULL THEN 'type (varchar(64))' END,
    CASE WHEN country_code IS NULL THEN 'country_code (char(2))' END,
    CASE WHEN created_at IS NULL THEN 'created_at (timestamp)' END,
    CASE WHEN updated_at IS NULL THEN 'updated_at (timestamp)' END
  ], ', ') as missing_columns

FROM (
  SELECT 
    TRY_CAST(id AS VARCHAR(36)) as id,
    TRY_CAST(name AS VARCHAR(255)) as name,
    TRY_CAST(type AS VARCHAR(64)) as type,
    TRY_CAST(country_code AS CHAR(2)) as country_code,
    TRY_CAST(created_at AS TIMESTAMP) as created_at,
    TRY_CAST(updated_at AS TIMESTAMP) as updated_at
  FROM {{ ref('int_entities_counterparty') }}
  LIMIT 1
) t
WHERE id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL
UNIO
N ALL

-- Test 5: Canonical identifier format validation
SELECT
  CASE 
    WHEN invalid_canonical_ids = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', invalid_canonical_ids, ' records with invalid canonical ID format')
  END as test_result,
  
  CONCAT('Invalid canonical IDs found: ', invalid_canonical_ids, ' out of ', total_records) as missing_columns

FROM (
  SELECT 
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NOT NULL 
        AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
      THEN 1 ELSE 0 
    END) as invalid_canonical_ids
  FROM (
    SELECT id FROM {{ ref('int_entities_company') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_fund') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_investor') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_counterparty') }}
  )
) canonical_validation
WHERE invalid_canonical_ids > 0

UNION ALL

-- Test 6: Cross-reference mapping validation
SELECT
  CASE 
    WHEN unmapped_entities = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', unmapped_entities, ' entities without cross-reference mapping')
  END as test_result,
  
  CONCAT('Entities without xref mapping: ', unmapped_entities, ' out of ', total_entities) as missing_columns

FROM (
  SELECT 
    COUNT(*) as total_entities,
    SUM(CASE 
      WHEN x.canonical_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as unmapped_entities
  FROM {{ ref('int_entities_company') }} c
  LEFT JOIN {{ ref('stg_ref_xref_companies') }} x ON c.id = x.canonical_company_id
) xref_validation
WHERE unmapped_entities > 0

UNION ALL

-- Test 7: Enhanced business rule validation for canonical identifiers
SELECT
  CASE 
    WHEN business_rule_violations = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', business_rule_violations, ' business rule violations')
  END as test_result,
  
  CONCAT('Business rule violations: ', business_rule_violations, ' out of ', total_records) as missing_columns

FROM (
  SELECT 
    COUNT(*) as total_records,
    SUM(CASE 
      -- Enhanced currency validation (must be exactly 3 characters and uppercase)
      WHEN currency IS NOT NULL AND (LENGTH(currency) != 3 OR currency != UPPER(currency)) THEN 1
      -- Enhanced UUID validation for industry_id
      WHEN industry_id IS NOT NULL 
        AND NOT REGEXP_LIKE(industry_id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') THEN 1
      -- Source system ID validation (must have at least one source ID)
      WHEN crm_company_id IS NULL AND pm_company_id IS NULL THEN 1
      ELSE 0 
    END) as business_rule_violations
  FROM {{ ref('int_entities_company') }}
) business_validation
WHERE business_rule_violations > 0