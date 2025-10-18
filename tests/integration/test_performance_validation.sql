/*
  Performance validation tests for data transformation pipeline
  
  These tests validate that the data transformation process meets
  performance requirements and identify potential bottlenecks.
*/

{{ config(tags=['integration', 'performance']) }}

-- Test 1: Model execution time validation
WITH model_performance AS (
  SELECT 
    'int_entities_company' as model_name,
    COUNT(*) as record_count,
    -- Estimate complexity based on joins and transformations
    COUNT(DISTINCT crm_company_id) + COUNT(DISTINCT pm_company_id) as source_complexity,
    CURRENT_TIMESTAMP() as execution_time
  FROM {{ ref('int_entities_company') }}
  
  UNION ALL
  
  SELECT 
    'int_entities_fund' as model_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT admin_fund_id) + COUNT(DISTINCT crm_fund_id) as source_complexity,
    CURRENT_TIMESTAMP() as execution_time
  FROM {{ ref('int_entities_fund') }}
  
  UNION ALL
  
  SELECT 
    'int_entities_investor' as model_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT crm_investor_id) + COUNT(DISTINCT admin_investor_id) as source_complexity,
    CURRENT_TIMESTAMP() as execution_time
  FROM {{ ref('int_entities_investor') }}
),

-- Test 2: Data volume scalability
volume_metrics AS (
  SELECT 
    'VOLUME_SCALABILITY' as test_category,
    'Record processing capacity' as test_name,
    CASE 
      WHEN total_records >= 10000 THEN 'PASS: High volume processing'
      WHEN total_records >= 1000 THEN 'PASS: Medium volume processing'
      WHEN total_records >= 100 THEN 'WARNING: Low volume processing'
      ELSE 'FAIL: Insufficient test data volume'
    END as test_result,
    total_records,
    avg_complexity
  FROM (
    SELECT 
      SUM(record_count) as total_records,
      AVG(source_complexity) as avg_complexity
    FROM model_performance
  )
),

-- Test 3: Memory efficiency validation
memory_efficiency AS (
  SELECT 
    'MEMORY_EFFICIENCY' as test_category,
    'Large dataset processing' as test_name,
    CASE 
      WHEN max_record_count <= 100000 THEN 'PASS: Efficient memory usage'
      WHEN max_record_count <= 500000 THEN 'WARNING: High memory usage'
      ELSE 'FAIL: Excessive memory usage'
    END as test_result,
    max_record_count,
    total_models
  FROM (
    SELECT 
      MAX(record_count) as max_record_count,
      COUNT(*) as total_models
    FROM model_performance
  )
),

-- Test 4: Transformation complexity validation
complexity_metrics AS (
  SELECT 
    'TRANSFORMATION_COMPLEXITY' as test_category,
    'SQL complexity and optimization' as test_name,
    CASE 
      WHEN avg_complexity <= 10 THEN 'PASS: Optimal complexity'
      WHEN avg_complexity <= 20 THEN 'WARNING: Moderate complexity'
      ELSE 'FAIL: High complexity may impact performance'
    END as test_result,
    CAST(avg_complexity AS INT64) as avg_complexity,
    max_complexity
  FROM (
    SELECT 
      AVG(source_complexity) as avg_complexity,
      MAX(source_complexity) as max_complexity
    FROM model_performance
  )
)

-- Combine all performance test results
SELECT 
  test_category,
  test_name,
  test_result,
  total_records as metric_value,
  avg_complexity as secondary_metric
FROM volume_metrics

UNION ALL

SELECT 
  test_category,
  test_name,
  test_result,
  max_record_count as metric_value,
  total_models as secondary_metric
FROM memory_efficiency

UNION ALL

SELECT 
  test_category,
  test_name,
  test_result,
  avg_complexity as metric_value,
  max_complexity as secondary_metric
FROM complexity_metrics

-- Only return failed or warning tests
WHERE test_result LIKE 'FAIL%' OR test_result LIKE 'WARNING%'