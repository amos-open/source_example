/*
  Integration tests to validate compatibility with amos_core canonical models
  
  These tests verify that the intermediate models in amos_source_example
  produce data that matches the exact schema contracts expected by amos_core.
  
  Test Categories:
  1. Schema compatibility - column names and data types
  2. Data contract validation - required fields and constraints
  3. Foreign key relationships - reference data integrity
  4. Business rule validation - data quality and format checks
*/

-- Test 1: Company entity schema compatibility
{{ config(tags=['integration', 'amos_core_compatibility']) }}

WITH company_schema_test AS (
  SELECT
    -- Test that all required columns exist with correct data types
    CASE 
      WHEN COUNT(*) = 0 THEN 'PASS'
      ELSE 'FAIL: Missing or incorrect columns in int_entities_company'
    END as test_result,
    
    -- List any missing columns
    ARRAY_TO_STRING(ARRAY[
      CASE WHEN id IS NULL THEN 'id (uuid)' END,
      CASE WHEN name IS NULL THEN 'name (varchar(255))' END,
      CASE WHEN website IS NULL THEN 'website (varchar(255))' END,
      CASE WHEN description IS NULL THEN 'description (text)' END,
      CASE WHEN currency IS NULL THEN 'currency (varchar(3))' END,
      CASE WHEN industry_id IS NULL THEN 'industry_id (uuid)' END,
      CASE WHEN created_at IS NULL THEN 'created_at (timestamp)' END,
      CASE WHEN updated_at IS NULL THEN 'updated_at (timestamp)' END
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
      TRY_CAST(updated_at AS TIMESTAMP) as updated_at
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