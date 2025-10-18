/*
  Foreign key relationship integrity tests across all entities
  
  These tests validate that all foreign key relationships are properly
  maintained and that referential integrity is preserved throughout
  the data transformation pipeline.
*/

{{ config(tags=['integration', 'foreign_key_integrity']) }}

-- Test 1: Entity to reference data relationships
WITH reference_data_integrity AS (
  -- Company to Industry relationship
  SELECT 
    'company_industry' as relationship_type,
    'Company industry_id references' as test_name,
    COUNT(*) as total_references,
    COUNT(i.id) as valid_references,
    COUNT(*) - COUNT(i.id) as broken_references,
    'All company industry_id values must reference valid industries' as constraint_description
  FROM {{ ref('int_entities_company') }} c
  LEFT JOIN {{ ref('stg_ref_industries') }} i ON c.industry_id = i.id
  WHERE c.industry_id IS NOT NULL
  
  UNION ALL
  
  -- Investor to Investor Type relationship
  SELECT 
    'investor_type' as relationship_type,
    'Investor investor_type_id references' as test_name,
    COUNT(*) as total_references,
    COUNT(it.id) as valid_references,
    COUNT(*) - COUNT(it.id) as broken_references,
    'All investor investor_type_id values must reference valid investor types' as constraint_description
  FROM {{ ref('int_entities_investor') }} inv
  LEFT JOIN {{ ref('stg_ref_investor_types') }} it ON inv.investor_type_id = it.id
  WHERE inv.investor_type_id IS NOT NULL
  
  UNION ALL
  
  -- Fund to Currency relationship
  SELECT 
    'fund_currency' as relationship_type,
    'Fund base_currency_code references' as test_name,
    COUNT(*) as total_references,
    COUNT(c.currency_code) as valid_references,
    COUNT(*) - COUNT(c.currency_code) as broken_references,
    'All fund base_currency_code values must reference valid currencies' as constraint_description
  FROM {{ ref('int_entities_fund') }} f
  LEFT JOIN {{ ref('stg_ref_currencies') }} c ON f.base_currency_code = c.currency_code
  WHERE f.base_currency_code IS NOT NULL
  
  UNION ALL
  
  -- Company to Currency relationship
  SELECT 
    'company_currency' as relationship_type,
    'Company currency references' as test_name,
    COUNT(*) as total_references,
    COUNT(c.currency_code) as valid_references,
    COUNT(*) - COUNT(c.currency_code) as broken_references,
    'All company currency values must reference valid currencies' as constraint_description
  FROM {{ ref('int_entities_company') }} comp
  LEFT JOIN {{ ref('stg_ref_currencies') }} c ON comp.currency = c.currency_code
  WHERE comp.currency IS NOT NULL
  
  UNION ALL
  
  -- Counterparty to Country relationship
  SELECT 
    'counterparty_country' as relationship_type,
    'Counterparty country_code references' as test_name,
    COUNT(*) as total_references,
    COUNT(c.country_code) as valid_references,
    COUNT(*) - COUNT(c.country_code) as broken_references,
    'All counterparty country_code values must reference valid countries' as constraint_description
  FROM {{ ref('int_entities_counterparty') }} cp
  LEFT JOIN {{ ref('stg_ref_countries') }} c ON cp.country_code = c.country_code
  WHERE cp.country_code IS NOT NULL
),

-- Test 2: Cross-reference mapping relationships
xref_mapping_integrity AS (
  -- Company to cross-reference mapping
  SELECT 
    'company_xref' as relationship_type,
    'Company canonical ID mapping' as test_name,
    COUNT(*) as total_references,
    COUNT(x.canonical_company_id) as valid_references,
    COUNT(*) - COUNT(x.canonical_company_id) as broken_references,
    'All companies must have corresponding cross-reference mapping' as constraint_description
  FROM {{ ref('int_entities_company') }} c
  LEFT JOIN {{ ref('stg_ref_xref_companies') }} x ON c.id = x.canonical_company_id
  
  UNION ALL
  
  -- Fund to cross-reference mapping
  SELECT 
    'fund_xref' as relationship_type,
    'Fund canonical ID mapping' as test_name,
    COUNT(*) as total_references,
    COUNT(x.canonical_fund_id) as valid_references,
    COUNT(*) - COUNT(x.canonical_fund_id) as broken_references,
    'All funds must have corresponding cross-reference mapping' as constraint_description
  FROM {{ ref('int_entities_fund') }} f
  LEFT JOIN {{ ref('stg_ref_xref_funds') }} x ON f.id = x.canonical_fund_id
  
  UNION ALL
  
  -- Investor to cross-reference mapping
  SELECT 
    'investor_xref' as relationship_type,
    'Investor canonical ID mapping' as test_name,
    COUNT(*) as total_references,
    COUNT(x.canonical_investor_id) as valid_references,
    COUNT(*) - COUNT(x.canonical_investor_id) as broken_references,
    'All investors must have corresponding cross-reference mapping' as constraint_description
  FROM {{ ref('int_entities_investor') }} i
  LEFT JOIN {{ ref('stg_ref_xref_investors') }} x ON i.id = x.canonical_investor_id
),

-- Test 3: Transactional data relationships
transactional_integrity AS (
  -- Investment to Company relationship
  SELECT 
    'investment_company' as relationship_type,
    'Investment company_id references' as test_name,
    COUNT(*) as total_references,
    COUNT(c.id) as valid_references,
    COUNT(*) - COUNT(c.id) as broken_references,
    'All investments must reference valid companies' as constraint_description
  FROM {{ ref('stg_pm_investments') }} inv
  LEFT JOIN {{ ref('int_entities_company') }} c ON inv.canonical_company_id = c.id
  WHERE inv.canonical_company_id IS NOT NULL
  
  UNION ALL
  
  -- Investment to Fund relationship (via cross-reference)
  SELECT 
    'investment_fund' as relationship_type,
    'Investment fund references' as test_name,
    COUNT(*) as total_references,
    COUNT(f.id) as valid_references,
    COUNT(*) - COUNT(f.id) as broken_references,
    'All investments should reference valid funds' as constraint_description
  FROM {{ ref('stg_pm_investments') }} inv
  LEFT JOIN {{ ref('stg_ref_xref_funds') }} xf ON inv.fund_id = xf.pm_fund_id
  LEFT JOIN {{ ref('int_entities_fund') }} f ON xf.canonical_fund_id = f.id
  WHERE inv.fund_id IS NOT NULL
  
  UNION ALL
  
  -- Valuation to Company relationship
  SELECT 
    'valuation_company' as relationship_type,
    'Valuation company_id references' as test_name,
    COUNT(*) as total_references,
    COUNT(c.id) as valid_references,
    COUNT(*) - COUNT(c.id) as broken_references,
    'All valuations must reference valid companies' as constraint_description
  FROM {{ ref('stg_pm_valuations') }} val
  LEFT JOIN {{ ref('int_entities_company') }} c ON val.canonical_company_id = c.id
  WHERE val.canonical_company_id IS NOT NULL
  
  UNION ALL
  
  -- Financial data to Company relationship
  SELECT 
    'financials_company' as relationship_type,
    'Company financials company_id references' as test_name,
    COUNT(*) as total_references,
    COUNT(c.id) as valid_references,
    COUNT(*) - COUNT(c.id) as broken_references,
    'All financial records must reference valid companies' as constraint_description
  FROM {{ ref('stg_pm_company_financials') }} fin
  LEFT JOIN {{ ref('int_entities_company') }} c ON fin.canonical_company_id = c.id
  WHERE fin.canonical_company_id IS NOT NULL
),

-- Test 4: Bridge table relationships
bridge_integrity AS (
  -- Company-Country bridge relationships
  SELECT 
    'company_country_bridge' as relationship_type,
    'Company-Country bridge integrity' as test_name,
    COUNT(*) as total_references,
    COUNT(CASE WHEN c.id IS NOT NULL AND co.country_code IS NOT NULL THEN 1 END) as valid_references,
    COUNT(*) - COUNT(CASE WHEN c.id IS NOT NULL AND co.country_code IS NOT NULL THEN 1 END) as broken_references,
    'All company-country bridge records must reference valid entities' as constraint_description
  FROM {{ ref('int_relationships_company_geography') }} bridge
  LEFT JOIN {{ ref('int_entities_company') }} c ON bridge.company_id = c.id
  LEFT JOIN {{ ref('stg_ref_countries') }} co ON bridge.country_code = co.country_code
  
  UNION ALL
  
  -- Company-Industry bridge relationships
  SELECT 
    'company_industry_bridge' as relationship_type,
    'Company-Industry bridge integrity' as test_name,
    COUNT(*) as total_references,
    COUNT(CASE WHEN c.id IS NOT NULL AND i.id IS NOT NULL THEN 1 END) as valid_references,
    COUNT(*) - COUNT(CASE WHEN c.id IS NOT NULL AND i.id IS NOT NULL THEN 1 END) as broken_references,
    'All company-industry bridge records must reference valid entities' as constraint_description
  FROM {{ ref('int_relationships_company_industry') }} bridge
  LEFT JOIN {{ ref('int_entities_company') }} c ON bridge.company_id = c.id
  LEFT JOIN {{ ref('stg_ref_industries') }} i ON bridge.industry_id = i.id
  
  UNION ALL
  
  -- Fund-Investment bridge relationships
  SELECT 
    'fund_investment_bridge' as relationship_type,
    'Fund-Investment bridge integrity' as test_name,
    COUNT(*) as total_references,
    COUNT(CASE WHEN f.id IS NOT NULL AND c.id IS NOT NULL THEN 1 END) as valid_references,
    COUNT(*) - COUNT(CASE WHEN f.id IS NOT NULL AND c.id IS NOT NULL THEN 1 END) as broken_references,
    'All fund-investment bridge records must reference valid entities' as constraint_description
  FROM {{ ref('int_relationships_fund_investment') }} bridge
  LEFT JOIN {{ ref('int_entities_fund') }} f ON bridge.fund_id = f.id
  LEFT JOIN {{ ref('int_entities_company') }} c ON bridge.company_id = c.id
  
  UNION ALL
  
  -- Fund-Investor bridge relationships
  SELECT 
    'fund_investor_bridge' as relationship_type,
    'Fund-Investor bridge integrity' as test_name,
    COUNT(*) as total_references,
    COUNT(CASE WHEN f.id IS NOT NULL AND i.id IS NOT NULL THEN 1 END) as valid_references,
    COUNT(*) - COUNT(CASE WHEN f.id IS NOT NULL AND i.id IS NOT NULL THEN 1 END) as broken_references,
    'All fund-investor bridge records must reference valid entities' as constraint_description
  FROM {{ ref('int_relationships_fund_investor') }} bridge
  LEFT JOIN {{ ref('int_entities_fund') }} f ON bridge.fund_id = f.id
  LEFT JOIN {{ ref('int_entities_investor') }} i ON bridge.investor_id = i.id
)

-- Combine all foreign key integrity test results
SELECT 
  relationship_type,
  test_name,
  CASE 
    WHEN broken_references = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', broken_references, ' broken references out of ', total_references, ' total')
  END as test_result,
  constraint_description,
  total_references,
  valid_references,
  broken_references
FROM reference_data_integrity
WHERE broken_references > 0

UNION ALL

SELECT 
  relationship_type,
  test_name,
  CASE 
    WHEN broken_references = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', broken_references, ' missing mappings out of ', total_references, ' total')
  END as test_result,
  constraint_description,
  total_references,
  valid_references,
  broken_references
FROM xref_mapping_integrity
WHERE broken_references > 0

UNION ALL

SELECT 
  relationship_type,
  test_name,
  CASE 
    WHEN broken_references = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', broken_references, ' broken transactional references out of ', total_references, ' total')
  END as test_result,
  constraint_description,
  total_references,
  valid_references,
  broken_references
FROM transactional_integrity
WHERE broken_references > 0

UNION ALL

SELECT 
  relationship_type,
  test_name,
  CASE 
    WHEN broken_references = 0 THEN 'PASS'
    ELSE CONCAT('FAIL: ', broken_references, ' broken bridge references out of ', total_references, ' total')
  END as test_result,
  constraint_description,
  total_references,
  valid_references,
  broken_references
FROM bridge_integrity
WHERE broken_references > 0

ORDER BY relationship_type, test_name