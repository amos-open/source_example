/*
  Enhanced seed data validation tests
  
  These tests validate that the enhanced seed data files contain all required
  canonical identifier columns and that the data meets quality standards
  for integration with the amos_core canonical models.
*/

{{ config(tags=['integration', 'seed_validation', 'enhanced']) }}

-- Test 1: Validate canonical identifier columns exist in all seed files
WITH seed_column_validation AS (
  -- CRM seed files validation
  SELECT 
    'crm_companies' as seed_table,
    'Required canonical columns present' as test_name,
    COUNT(*) as total_records,
    COUNT(crm_company_id) as crm_id_count,
    COUNT(canonical_company_id) as canonical_id_count,
    SUM(CASE 
      WHEN crm_company_id IS NULL OR canonical_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('seed_crm_companies') }}
  
  UNION ALL
  
  SELECT 
    'crm_contacts' as seed_table,
    'Required canonical columns present' as test_name,
    COUNT(*) as total_records,
    COUNT(crm_investor_id) as crm_id_count,
    COUNT(canonical_investor_id) as canonical_id_count,
    SUM(CASE 
      WHEN crm_investor_id IS NULL OR canonical_investor_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('seed_crm_contacts') }}
  
  UNION ALL
  
  -- Admin seed files validation
  SELECT 
    'admin_funds' as seed_table,
    'Required canonical columns present' as test_name,
    COUNT(*) as total_records,
    COUNT(admin_fund_id) as crm_id_count,
    COUNT(canonical_fund_id) as canonical_id_count,
    SUM(CASE 
      WHEN admin_fund_id IS NULL OR canonical_fund_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('seed_admin_funds') }}
  
  UNION ALL
  
  SELECT 
    'admin_investors' as seed_table,
    'Required canonical columns present' as test_name,
    COUNT(*) as total_records,
    COUNT(admin_investor_id) as crm_id_count,
    COUNT(canonical_investor_id) as canonical_id_count,
    SUM(CASE 
      WHEN admin_investor_id IS NULL OR canonical_investor_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('seed_admin_investors') }}
  
  UNION ALL
  
  -- PM seed files validation
  SELECT 
    'pm_investments' as seed_table,
    'Required canonical columns present' as test_name,
    COUNT(*) as total_records,
    COUNT(pm_investment_id) as crm_id_count,
    COUNT(canonical_company_id) as canonical_id_count,
    SUM(CASE 
      WHEN pm_investment_id IS NULL OR canonical_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('seed_pm_investments') }}
),

-- Test 2: Validate canonical ID format in seed files
canonical_format_validation AS (
  SELECT 
    'canonical_id_format' as validation_type,
    'UUID format validation in seed files' as test_name,
    COUNT(*) as total_canonical_ids,
    SUM(CASE 
      WHEN canonical_company_id IS NOT NULL 
        AND NOT REGEXP_LIKE(canonical_company_id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
      THEN 1 ELSE 0 
    END) as invalid_format_count,
    0 as missing_id_records -- placeholder for consistency
  FROM (
    SELECT canonical_company_id FROM {{ ref('seed_crm_companies') }}
    UNION ALL
    SELECT canonical_company_id FROM {{ ref('seed_pm_investments') }}
    UNION ALL
    SELECT canonical_fund_id FROM {{ ref('seed_admin_funds') }}
    UNION ALL
    SELECT canonical_investor_id FROM {{ ref('seed_crm_contacts') }}
    UNION ALL
    SELECT canonical_investor_id FROM {{ ref('seed_admin_investors') }}
  )
),

-- Test 3: Cross-reference mapping completeness
xref_completeness_validation AS (
  -- Company cross-reference completeness
  SELECT 
    'xref_companies' as validation_type,
    'Cross-reference mapping completeness' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT canonical_company_id) as canonical_id_count,
    SUM(CASE 
      WHEN canonical_company_id IS NULL 
        OR (crm_company_id IS NULL AND pm_company_id IS NULL AND accounting_entity_id IS NULL)
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('stg_ref_xref_companies') }}
  
  UNION ALL
  
  -- Fund cross-reference completeness
  SELECT 
    'xref_funds' as validation_type,
    'Cross-reference mapping completeness' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT canonical_fund_id) as canonical_id_count,
    SUM(CASE 
      WHEN canonical_fund_id IS NULL 
        OR (admin_fund_id IS NULL AND crm_fund_id IS NULL)
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('stg_ref_xref_funds') }}
  
  UNION ALL
  
  -- Investor cross-reference completeness
  SELECT 
    'xref_investors' as validation_type,
    'Cross-reference mapping completeness' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT canonical_investor_id) as canonical_id_count,
    SUM(CASE 
      WHEN canonical_investor_id IS NULL 
        OR (crm_investor_id IS NULL AND admin_investor_id IS NULL)
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('stg_ref_xref_investors') }}
),

-- Test 4: Data quality and consistency validation
data_quality_validation AS (
  SELECT 
    'data_quality' as validation_type,
    'Enhanced data quality standards' as test_name,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN data_quality_rating NOT IN ('HIGH_QUALITY', 'MEDIUM_QUALITY', 'LOW_QUALITY') 
      THEN 1 ELSE 0 
    END) as canonical_id_count,
    SUM(CASE 
      WHEN standardized_confidence NOT IN ('HIGH', 'MEDIUM', 'LOW') 
        OR source_systems_count <= 0
        OR primary_source_system IS NULL
      THEN 1 ELSE 0 
    END) as missing_id_records
  FROM {{ ref('stg_ref_xref_companies') }}
  
  UNION ALL
  
  SELECT 
    'currency_validation' as validation_type,
    'Currency code format validation' as test_name,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN base_currency_code IS NOT NULL 
        AND (LENGTH(base_currency_code) != 3 OR base_currency_code != UPPER(base_currency_code))
      THEN 1 ELSE 0 
    END) as canonical_id_count,
    0 as missing_id_records
  FROM {{ ref('seed_admin_funds') }}
)

-- Combine all validation results and show only failures
SELECT 
  seed_table as test_category,
  test_name,
  CASE 
    WHEN missing_id_records > 0 THEN CONCAT('FAIL: ', missing_id_records, ' records missing required IDs')
    WHEN canonical_id_count != total_records THEN CONCAT('FAIL: ID count mismatch - expected ', total_records, ', got ', canonical_id_count)
    ELSE 'PASS'
  END as test_result,
  total_records,
  missing_id_records as failure_count
FROM seed_column_validation
WHERE missing_id_records > 0 OR canonical_id_count != total_records

UNION ALL

SELECT 
  validation_type as test_category,
  test_name,
  CASE 
    WHEN invalid_format_count > 0 THEN CONCAT('FAIL: ', invalid_format_count, ' invalid UUID formats')
    ELSE 'PASS'
  END as test_result,
  total_canonical_ids as total_records,
  invalid_format_count as failure_count
FROM canonical_format_validation
WHERE invalid_format_count > 0

UNION ALL

SELECT 
  validation_type as test_category,
  test_name,
  CASE 
    WHEN missing_id_records > 0 THEN CONCAT('FAIL: ', missing_id_records, ' incomplete cross-reference mappings')
    WHEN canonical_id_count = 0 THEN 'FAIL: No canonical IDs found'
    ELSE 'PASS'
  END as test_result,
  total_records,
  missing_id_records as failure_count
FROM xref_completeness_validation
WHERE missing_id_records > 0 OR canonical_id_count = 0

UNION ALL

SELECT 
  validation_type as test_category,
  test_name,
  CASE 
    WHEN canonical_id_count > 0 THEN CONCAT('FAIL: ', canonical_id_count, ' data quality violations')
    WHEN missing_id_records > 0 THEN CONCAT('FAIL: ', missing_id_records, ' metadata quality issues')
    ELSE 'PASS'
  END as test_result,
  total_records,
  canonical_id_count + missing_id_records as failure_count
FROM data_quality_validation
WHERE canonical_id_count > 0 OR missing_id_records > 0