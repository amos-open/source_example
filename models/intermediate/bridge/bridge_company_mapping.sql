{{
  config(
    materialized='view',
    tags=['intermediate', 'bridge', 'company']
  )
}}

/*
  Bridge transformation model for company entity mapping
  
  This model handles column name mismatches between staging CRM companies
  and the intermediate company entity model, providing explicit field
  mapping and data type conversions.
  
  Purpose:
  - Map staging column names to intermediate model expectations
  - Handle data type conversions and casting
  - Apply cross-reference lookups for canonical identifiers
  - Provide fallback logic for missing data
  
  Input: stg_crm_companies, stg_ref_xref_companies, PM data sources
  Output: Standardized intermediate company entity structure
*/

WITH crm_companies AS (
    SELECT * FROM {{ ref('stg_crm_companies') }}
),

pm_company_summary AS (
    SELECT
        company_id as pm_company_id,
        company_name,
        count(*) as investment_count,
        sum(investment_amount) as total_investment_amount,
        min(investment_date) as first_investment_date,
        max(investment_date) as latest_investment_date
    FROM {{ ref('stg_pm_investments') }}
    WHERE company_id IS NOT NULL
    GROUP BY company_id, company_name
),

xref_companies AS (
    SELECT * FROM {{ ref('stg_ref_xref_companies') }}
    WHERE data_quality_rating IN ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

-- Apply bridge transformation mapping
bridge_mapped AS (
    SELECT
        {{ alias_staging_columns('company') }}
    FROM crm_companies c
    LEFT JOIN xref_companies x ON x.crm_company_id = c.company_id
    LEFT JOIN pm_company_summary pm ON x.pm_company_id = pm.pm_company_id
),

-- Add derived fields and business logic
enhanced_mapping AS (
    SELECT
        *,
        
        -- Source system tracking
        CASE 
            WHEN crm_company_id IS NOT NULL THEN 'CRM_VENDOR'
            ELSE 'UNKNOWN'
        END as primary_data_source,
        
        CASE 
            when crm_company_id IS NOT NULL AND pm_company_id IS NOT NULL THEN 'MULTI_SOURCE'
            WHEN crm_company_id IS NOT NULL THEN 'CRM_ONLY'
            ELSE 'NO_SOURCE'
        END as source_coverage,
        
        -- Investment status derivation
        CASE 
            WHEN investment_count > 0 THEN 'PORTFOLIO_COMPANY'
            ELSE 'PROSPECT'
        END as investment_status,
        
        -- Data completeness assessment
        (
            CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN industry_primary IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN founded_year IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN employee_count IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END
        ) / 7.0 * 100 as overall_completeness_score,
        
        -- Processing metadata
        CURRENT_TIMESTAMP() as processed_at,
        'BRIDGE_TRANSFORMATION' as transformation_type
        
    FROM bridge_mapped
    WHERE id IS NOT NULL  -- Ensure canonical ID exists
)

SELECT * FROM enhanced_mapping