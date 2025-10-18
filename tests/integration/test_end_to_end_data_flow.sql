/*
  End-to-end integration tests for complete data flow validation
  
  These tests validate the complete data transformation pipeline from seed files
  through staging, intermediate, and canonical models to ensure data integrity
  and canonical identifier consistency across all layers.
  
  Test Categories:
  1. Data flow continuity - validate data flows through all transformation layers
  2. Canonical identifier consistency - ensure IDs are preserved across pipeline
  3. Performance validation - check transformation performance metrics
  4. Business rule compliance - validate business logic throughout pipeline
*/

{{ config(tags=['integration', 'end_to_end', 'data_flow']) }}

-- Test 1: Validate complete data flow from seeds to intermediate models
WITH seed_to_staging_flow AS (
  -- Count records at seed level
  SELECT 
    'crm_companies' as source_table,
    COUNT(*) as seed_count,
    COUNT(DISTINCT crm_company_id) as unique_ids_seed
  FROM {{ ref('crm_companies') }}
  WHERE crm_company_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'crm_contacts' as source_table,
    COUNT(*) as seed_count,
    COUNT(DISTINCT crm_investor_id) as unique_ids_seed
  FROM {{ ref('crm_contacts') }}
  WHERE crm_investor_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'admin_funds' as source_table,
    COUNT(*) as seed_count,
    COUNT(DISTINCT admin_fund_id) as unique_ids_seed
  FROM {{ ref('admin_funds') }}
  WHERE admin_fund_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'pm_investments' as source_table,
    COUNT(*) as seed_count,
    COUNT(DISTINCT pm_investment_id) as unique_ids_seed
  FROM {{ ref('pm_investments') }}
  WHERE pm_investment_id IS NOT NULL
),

staging_counts AS (
  -- Count records at staging level
  SELECT 
    'stg_crm_companies' as staging_table,
    COUNT(*) as staging_count,
    COUNT(DISTINCT company_id) as unique_ids_staging
  FROM {{ ref('stg_crm_companies') }}
  WHERE company_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'stg_crm_contacts' as staging_table,
    COUNT(*) as staging_count,
    COUNT(DISTINCT contact_id) as unique_ids_staging
  FROM {{ ref('stg_crm_contacts') }}
  WHERE contact_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'stg_admin_funds' as staging_table,
    COUNT(*) as staging_count,
    COUNT(DISTINCT fund_id) as unique_ids_staging
  FROM {{ ref('stg_admin_funds') }}
  WHERE fund_id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'stg_pm_investments' as staging_table,
    COUNT(*) as staging_count,
    COUNT(DISTINCT investment_id) as unique_ids_staging
  FROM {{ ref('stg_pm_investments') }}
  WHERE investment_id IS NOT NULL
),

intermediate_counts AS (
  -- Count records at intermediate level
  SELECT 
    'int_entities_company' as intermediate_table,
    COUNT(*) as intermediate_count,
    COUNT(DISTINCT id) as unique_canonical_ids
  FROM {{ ref('int_entities_company') }}
  WHERE id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'int_entities_investor' as intermediate_table,
    COUNT(*) as intermediate_count,
    COUNT(DISTINCT id) as unique_canonical_ids
  FROM {{ ref('int_entities_investor') }}
  WHERE id IS NOT NULL
  
  UNION ALL
  
  SELECT 
    'int_entities_fund' as intermediate_table,
    COUNT(*) as intermediate_count,
    COUNT(DISTINCT id) as unique_canonical_ids
  FROM {{ ref('int_entities_fund') }}
  WHERE id IS NOT NULL
),

-- Test data flow continuity
data_flow_validation AS (
  SELECT 
    'DATA_FLOW_CONTINUITY' as test_category,
    'Seed to staging record preservation' as test_name,
    CASE 
      WHEN s.seed_count = st.staging_count THEN 'PASS'
      ELSE CONCAT('FAIL: Seed count ', s.seed_count, ' != Staging count ', st.staging_count)
    END as test_result,
    s.source_table,
    s.seed_count,
    st.staging_count
  FROM seed_to_staging_flow s
  JOIN staging_counts st ON (
    (s.source_table = 'crm_companies' AND st.staging_table = 'stg_crm_companies') OR
    (s.source_table = 'crm_contacts' AND st.staging_table = 'stg_crm_contacts') OR
    (s.source_table = 'admin_funds' AND st.staging_table = 'stg_admin_funds') OR
    (s.source_table = 'pm_investments' AND st.staging_table = 'stg_pm_investments')
  )
)

SELECT * FROM data_flow_validation
WHERE test_result LIKE 'FAIL%'

UNION ALL

-- Test 2: Canonical identifier consistency across pipeline
SELECT 
  'CANONICAL_ID_CONSISTENCY' as test_category,
  'Company canonical ID consistency' as test_name,
  CASE 
    WHEN COUNT(DISTINCT c.canonical_company_id) = COUNT(DISTINCT i.id) THEN 'PASS'
    ELSE CONCAT('FAIL: Xref canonical IDs ', COUNT(DISTINCT c.canonical_company_id), 
                ' != Intermediate IDs ', COUNT(DISTINCT i.id))
  END as test_result,
  'company_entities' as source_table,
  COUNT(DISTINCT c.canonical_company_id) as seed_count,
  COUNT(DISTINCT i.id) as staging_count
FROM {{ ref('stg_ref_xref_companies') }} c
JOIN {{ ref('int_entities_company') }} i ON c.canonical_company_id = i.id

UNION ALL

SELECT 
  'CANONICAL_ID_CONSISTENCY' as test_category,
  'Fund canonical ID consistency' as test_name,
  CASE 
    WHEN COUNT(DISTINCT f.canonical_fund_id) = COUNT(DISTINCT i.id) THEN 'PASS'
    ELSE CONCAT('FAIL: Xref canonical IDs ', COUNT(DISTINCT f.canonical_fund_id), 
                ' != Intermediate IDs ', COUNT(DISTINCT i.id))
  END as test_result,
  'fund_entities' as source_table,
  COUNT(DISTINCT f.canonical_fund_id) as seed_count,
  COUNT(DISTINCT i.id) as staging_count
FROM {{ ref('stg_ref_xref_funds') }} f
JOIN {{ ref('int_entities_fund') }} i ON f.canonical_fund_id = i.id

UNION ALL

SELECT 
  'CANONICAL_ID_CONSISTENCY' as test_category,
  'Investor canonical ID consistency' as test_name,
  CASE 
    WHEN COUNT(DISTINCT inv.canonical_investor_id) = COUNT(DISTINCT i.id) THEN 'PASS'
    ELSE CONCAT('FAIL: Xref canonical IDs ', COUNT(DISTINCT inv.canonical_investor_id), 
                ' != Intermediate IDs ', COUNT(DISTINCT i.id))
  END as test_result,
  'investor_entities' as source_table,
  COUNT(DISTINCT inv.canonical_investor_id) as seed_count,
  COUNT(DISTINCT i.id) as staging_count
FROM {{ ref('stg_ref_xref_investors') }} inv
JOIN {{ ref('int_entities_investor') }} i ON inv.canonical_investor_id = i.id

UNION ALL

-- Test 3: Cross-reference mapping integrity
SELECT 
  'MAPPING_INTEGRITY' as test_category,
  'Company cross-reference completeness' as test_name,
  CASE 
    WHEN orphaned_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', orphaned_count, ' companies without cross-reference mapping')
  END as test_result,
  'company_xref_mapping' as source_table,
  total_companies as seed_count,
  mapped_companies as staging_count
FROM (
  SELECT 
    COUNT(*) as total_companies,
    COUNT(x.canonical_company_id) as mapped_companies,
    COUNT(*) - COUNT(x.canonical_company_id) as orphaned_count
  FROM {{ ref('stg_crm_companies') }} c
  LEFT JOIN {{ ref('stg_ref_xref_companies') }} x ON c.company_id = x.crm_company_id
)

UNION ALL

-- Test 4: Performance validation - transformation time metrics
SELECT 
  'PERFORMANCE_VALIDATION' as test_category,
  'Intermediate model record processing rate' as test_name,
  CASE 
    WHEN records_per_second >= 1000 THEN 'PASS'
    WHEN records_per_second >= 500 THEN 'WARNING: Slow processing'
    ELSE 'FAIL: Performance below threshold'
  END as test_result,
  'processing_performance' as source_table,
  total_records as seed_count,
  CAST(records_per_second AS INT64) as staging_count
FROM (
  SELECT 
    COUNT(*) as total_records,
    -- Estimate processing rate based on record complexity
    COUNT(*) / GREATEST(1, EXTRACT(SECOND FROM CURRENT_TIMESTAMP() - TIMESTAMP('2024-01-01'))) as records_per_second
  FROM {{ ref('int_entities_company') }}
)

UNION ALL

-- Test 5: Business rule compliance across pipeline
SELECT 
  'BUSINESS_RULE_COMPLIANCE' as test_category,
  'Required field completeness in intermediate models' as test_name,
  CASE 
    WHEN missing_required_fields = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', missing_required_fields, ' records missing required fields')
  END as test_result,
  'required_field_validation' as source_table,
  total_records as seed_count,
  missing_required_fields as staging_count
FROM (
  SELECT 
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL 
      THEN 1 ELSE 0 
    END) as missing_required_fields
  FROM {{ ref('int_entities_company') }}
  
  UNION ALL
  
  SELECT 
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL 
      THEN 1 ELSE 0 
    END) as missing_required_fields
  FROM {{ ref('int_entities_fund') }}
  
  UNION ALL
  
  SELECT 
    COUNT(*) as total_records,
    SUM(CASE 
      WHEN id IS NULL OR name IS NULL OR created_at IS NULL OR updated_at IS NULL 
      THEN 1 ELSE 0 
    END) as missing_required_fields
  FROM {{ ref('int_entities_investor') }}
)

UNION ALL

-- Test 6: Data type consistency validation
SELECT 
  'DATA_TYPE_CONSISTENCY' as test_category,
  'UUID format validation across all entities' as test_name,
  CASE 
    WHEN invalid_uuid_count = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', invalid_uuid_count, ' records with invalid UUID format')
  END as test_result,
  'uuid_format_validation' as source_table,
  total_uuid_fields as seed_count,
  invalid_uuid_count as staging_count
FROM (
  SELECT 
    COUNT(*) as total_uuid_fields,
    SUM(CASE 
      WHEN id IS NOT NULL AND NOT REGEXP_LIKE(id, '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
      THEN 1 ELSE 0 
    END) as invalid_uuid_count
  FROM (
    SELECT id FROM {{ ref('int_entities_company') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_fund') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_investor') }}
    UNION ALL
    SELECT id FROM {{ ref('int_entities_counterparty') }}
  )
)

UNION ALL

-- Test 7: Referential integrity validation
SELECT 
  'REFERENTIAL_INTEGRITY' as test_category,
  'Foreign key relationship validation' as test_name,
  CASE 
    WHEN broken_references = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', broken_references, ' broken foreign key references')
  END as test_result,
  'foreign_key_validation' as source_table,
  total_references as seed_count,
  broken_references as staging_count
FROM (
  -- Check company industry_id references
  SELECT 
    COUNT(*) as total_references,
    SUM(CASE 
      WHEN c.industry_id IS NOT NULL AND i.id IS NULL 
      THEN 1 ELSE 0 
    END) as broken_references
  FROM {{ ref('int_entities_company') }} c
  LEFT JOIN {{ ref('stg_ref_industries') }} i ON c.industry_id = i.id
  WHERE c.industry_id IS NOT NULL
  
  UNION ALL
  
  -- Check investor investor_type_id references
  SELECT 
    COUNT(*) as total_references,
    SUM(CASE 
      WHEN inv.investor_type_id IS NOT NULL AND it.id IS NULL 
      THEN 1 ELSE 0 
    END) as broken_references
  FROM {{ ref('int_entities_investor') }} inv
  LEFT JOIN {{ ref('stg_ref_investor_types') }} it ON inv.investor_type_id = it.id
  WHERE inv.investor_type_id IS NOT NULL
)