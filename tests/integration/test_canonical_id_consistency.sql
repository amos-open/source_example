/*
  Canonical identifier consistency tests across the entire pipeline
  
  These tests validate that canonical identifiers are properly generated,
  maintained, and referenced consistently throughout the data transformation
  pipeline from seeds through intermediate models.
*/

{{ config(tags=['integration', 'canonical_ids']) }}

-- Test 1: Canonical ID generation and uniqueness
WITH canonical_id_validation AS (
  -- Company canonical IDs
  SELECT 
    'COMPANY_CANONICAL_IDS' as entity_type,
    'Canonical ID uniqueness and format' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT canonical_company_id) as unique_canonical_ids,
    SUM(CASE 
      WHEN canonical_company_id IS NULL THEN 1 
      ELSE 0 
    END) as null_canonical_ids,
    SUM(CASE 
      WHEN canonical_company_id IS NOT NULL 
        AND NOT REGEXP_LIKE(canonical_company_id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
      THEN 1 ELSE 0 
    END) as invalid_format_ids
  FROM {{ ref('stg_ref_xref_companies') }}
  
  UNION ALL
  
  -- Fund canonical IDs
  SELECT 
    'FUND_CANONICAL_IDS' as entity_type,
    'Canonical ID uniqueness and format' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT canonical_fund_id) as unique_canonical_ids,
    SUM(CASE 
      WHEN canonical_fund_id IS NULL THEN 1 
      ELSE 0 
    END) as null_canonical_ids,
    SUM(CASE 
      WHEN canonical_fund_id IS NOT NULL 
        AND NOT REGEXP_LIKE(canonical_fund_id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
      THEN 1 ELSE 0 
    END) as invalid_format_ids
  FROM {{ ref('stg_ref_xref_funds') }}
  
  UNION ALL
  
  -- Investor canonical IDs
  SELECT 
    'INVESTOR_CANONICAL_IDS' as entity_type,
    'Canonical ID uniqueness and format' as test_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT canonical_investor_id) as unique_canonical_ids,
    SUM(CASE 
      WHEN canonical_investor_id IS NULL THEN 1 
      ELSE 0 
    END) as null_canonical_ids,
    SUM(CASE 
      WHEN canonical_investor_id IS NOT NULL 
        AND NOT REGEXP_LIKE(canonical_investor_id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
      THEN 1 ELSE 0 
    END) as invalid_format_ids
  FROM {{ ref('stg_ref_xref_investors') }}
),

-- Test 2: Cross-reference mapping completeness
mapping_completeness AS (
  -- Company mapping completeness
  SELECT 
    'COMPANY_MAPPING' as mapping_type,
    'Source system to canonical mapping coverage' as test_name,
    COUNT(DISTINCT crm_company_id) as crm_source_count,
    COUNT(DISTINCT pm_company_id) as pm_source_count,
    COUNT(DISTINCT canonical_company_id) as canonical_count,
    SUM(CASE 
      WHEN crm_company_id IS NOT NULL AND canonical_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as unmapped_crm_records,
    SUM(CASE 
      WHEN pm_company_id IS NOT NULL AND canonical_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as unmapped_pm_records
  FROM {{ ref('stg_ref_xref_companies') }}
  
  UNION ALL
  
  -- Fund mapping completeness
  SELECT 
    'FUND_MAPPING' as mapping_type,
    'Source system to canonical mapping coverage' as test_name,
    COUNT(DISTINCT admin_fund_id) as crm_source_count,
    COUNT(DISTINCT crm_fund_id) as pm_source_count,
    COUNT(DISTINCT canonical_fund_id) as canonical_count,
    SUM(CASE 
      WHEN admin_fund_id IS NOT NULL AND canonical_fund_id IS NULL 
      THEN 1 ELSE 0 
    END) as unmapped_crm_records,
    SUM(CASE 
      WHEN crm_fund_id IS NOT NULL AND canonical_fund_id IS NULL 
      THEN 1 ELSE 0 
    END) as unmapped_pm_records
  FROM {{ ref('stg_ref_xref_funds') }}
  
  UNION ALL
  
  -- Investor mapping completeness
  SELECT 
    'INVESTOR_MAPPING' as mapping_type,
    'Source system to canonical mapping coverage' as test_name,
    COUNT(DISTINCT crm_investor_id) as crm_source_count,
    COUNT(DISTINCT admin_investor_id) as pm_source_count,
    COUNT(DISTINCT canonical_investor_id) as canonical_count,
    SUM(CASE 
      WHEN crm_investor_id IS NOT NULL AND canonical_investor_id IS NULL 
      THEN 1 ELSE 0 
    END) as unmapped_crm_records,
    SUM(CASE 
      WHEN admin_investor_id IS NOT NULL AND canonical_investor_id IS NULL 
      THEN 1 ELSE 0 
    END) as unmapped_pm_records
  FROM {{ ref('stg_ref_xref_investors') }}
),

-- Test 3: Intermediate model canonical ID consistency
intermediate_consistency AS (
  -- Company intermediate model consistency
  SELECT 
    'COMPANY_INTERMEDIATE' as consistency_type,
    'Intermediate model uses canonical IDs' as test_name,
    COUNT(*) as total_intermediate_records,
    COUNT(DISTINCT id) as unique_intermediate_ids,
    SUM(CASE 
      WHEN x.canonical_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_xref_mapping
  FROM {{ ref('int_entities_company') }} i
  LEFT JOIN {{ ref('stg_ref_xref_companies') }} x ON i.id = x.canonical_company_id
  
  UNION ALL
  
  -- Fund intermediate model consistency
  SELECT 
    'FUND_INTERMEDIATE' as consistency_type,
    'Intermediate model uses canonical IDs' as test_name,
    COUNT(*) as total_intermediate_records,
    COUNT(DISTINCT id) as unique_intermediate_ids,
    SUM(CASE 
      WHEN x.canonical_fund_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_xref_mapping
  FROM {{ ref('int_entities_fund') }} i
  LEFT JOIN {{ ref('stg_ref_xref_funds') }} x ON i.id = x.canonical_fund_id
  
  UNION ALL
  
  -- Investor intermediate model consistency
  SELECT 
    'INVESTOR_INTERMEDIATE' as consistency_type,
    'Intermediate model uses canonical IDs' as test_name,
    COUNT(*) as total_intermediate_records,
    COUNT(DISTINCT id) as unique_intermediate_ids,
    SUM(CASE 
      WHEN x.canonical_investor_id IS NULL 
      THEN 1 ELSE 0 
    END) as missing_xref_mapping
  FROM {{ ref('int_entities_investor') }} i
  LEFT JOIN {{ ref('stg_ref_xref_investors') }} x ON i.id = x.canonical_investor_id
),

-- Test 4: Source system ID preservation
source_id_preservation AS (
  -- Company source ID preservation
  SELECT 
    'COMPANY_SOURCE_IDS' as preservation_type,
    'Source system IDs preserved in intermediate models' as test_name,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN crm_company_id IS NOT NULL THEN 1 
      ELSE 0 
    END) as crm_ids_preserved,
    SUM(CASE 
      WHEN pm_company_id IS NOT NULL THEN 1 
      ELSE 0 
    END) as pm_ids_preserved,
    SUM(CASE 
      WHEN crm_company_id IS NULL AND pm_company_id IS NULL 
      THEN 1 ELSE 0 
    END) as records_without_source_ids
  FROM {{ ref('int_entities_company') }}
  
  UNION ALL
  
  -- Fund source ID preservation
  SELECT 
    'FUND_SOURCE_IDS' as preservation_type,
    'Source system IDs preserved in intermediate models' as test_name,
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN admin_fund_id IS NOT NULL THEN 1 
      ELSE 0 
    END) as crm_ids_preserved,
    SUM(CASE 
      WHEN crm_fund_id IS NOT NULL THEN 1 
      ELSE 0 
    END) as pm_ids_preserved,
    SUM(CASE 
      WHEN admin_fund_id IS NULL AND crm_fund_id IS NULL 
      THEN 1 ELSE 0 
    END) as records_without_source_ids
  FROM {{ ref('int_entities_fund') }}
)

-- Combine all test results and filter for failures
SELECT 
  entity_type as test_category,
  test_name,
  CASE 
    WHEN null_canonical_ids > 0 THEN CONCAT('FAIL: ', null_canonical_ids, ' null canonical IDs')
    WHEN invalid_format_ids > 0 THEN CONCAT('FAIL: ', invalid_format_ids, ' invalid UUID formats')
    WHEN total_records != unique_canonical_ids THEN CONCAT('FAIL: ', total_records - unique_canonical_ids, ' duplicate canonical IDs')
    ELSE 'PASS'
  END as test_result,
  total_records,
  unique_canonical_ids
FROM canonical_id_validation

UNION ALL

SELECT 
  mapping_type as test_category,
  test_name,
  CASE 
    WHEN unmapped_crm_records > 0 THEN CONCAT('FAIL: ', unmapped_crm_records, ' unmapped CRM records')
    WHEN unmapped_pm_records > 0 THEN CONCAT('FAIL: ', unmapped_pm_records, ' unmapped PM records')
    ELSE 'PASS'
  END as test_result,
  canonical_count as total_records,
  crm_source_count + pm_source_count as unique_canonical_ids
FROM mapping_completeness

UNION ALL

SELECT 
  consistency_type as test_category,
  test_name,
  CASE 
    WHEN missing_xref_mapping > 0 THEN CONCAT('FAIL: ', missing_xref_mapping, ' intermediate records without xref mapping')
    WHEN total_intermediate_records != unique_intermediate_ids THEN CONCAT('FAIL: ', total_intermediate_records - unique_intermediate_ids, ' duplicate intermediate IDs')
    ELSE 'PASS'
  END as test_result,
  total_intermediate_records,
  unique_intermediate_ids
FROM intermediate_consistency

UNION ALL

SELECT 
  preservation_type as test_category,
  test_name,
  CASE 
    WHEN records_without_source_ids > 0 THEN CONCAT('WARNING: ', records_without_source_ids, ' records without source system IDs')
    ELSE 'PASS'
  END as test_result,
  total_records,
  crm_ids_preserved + pm_ids_preserved as unique_canonical_ids
FROM source_id_preservation

-- Only show failed or warning tests
WHERE test_result LIKE 'FAIL%' OR test_result LIKE 'WARNING%'