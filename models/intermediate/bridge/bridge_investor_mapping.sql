{{
  config(
    materialized='view',
    tags=['intermediate', 'bridge', 'investor']
  )
}}

/*
  Bridge transformation model for investor entity mapping
  
  This model handles column name mismatches between staging admin investors
  and the intermediate investor entity model, providing explicit field
  mapping and data type conversions.
  
  Purpose:
  - Map staging column names to intermediate model expectations
  - Handle investor type standardization and classification
  - Apply cross-reference lookups for canonical identifiers
  - Generate investor profiling and scoring logic
  
  Input: stg_admin_investors, stg_ref_xref_investors
  Output: Standardized intermediate investor entity structure
*/

WITH admin_investors AS (
    SELECT * FROM {{ ref('stg_admin_investors') }}
),

xref_investors AS (
    SELECT * FROM {{ ref('stg_ref_xref_investors') }}
),

-- Apply bridge transformation mapping
bridge_mapped AS (
    SELECT
        {{ alias_staging_columns('investor') }},
        -- bring through additional fields used later that may not be in the alias set
        i.has_esg_requirements,
        i.compliance_status,
        i.risk_tolerance,
        i.investment_capacity
    FROM admin_investors i
    LEFT JOIN xref_investors x ON x.admin_investor_code = i.investor_code
),

-- Add derived fields and investor profiling
enhanced_mapping AS (
    SELECT
        *,
        
        -- Investor size categorization (already defined in alias_staging_columns)
        -- investor_size_category is already included from the macro
        
        -- Geographic region mapping
        CASE 
            WHEN standardized_country_code IN ('US', 'CA') THEN 'NORTH_AMERICA'
            WHEN standardized_country_code IN ('GB', 'DE', 'FR', 'NL', 'CH', 'IT', 'ES') THEN 'EUROPE'
            WHEN standardized_country_code IN ('JP', 'SG', 'HK', 'AU', 'KR') THEN 'ASIA_PACIFIC'
            WHEN standardized_country_code IN ('AE', 'SA', 'QA') THEN 'MIDDLE_EAST'
            ELSE 'OTHER'
        END as geographic_region,
        
        -- Investment profile assessment
        CASE 
            WHEN standardized_investor_type IN ('PENSION_FUND', 'ENDOWMENT', 'SOVEREIGN_WEALTH_FUND') 
            THEN 'TIER_1_INSTITUTIONAL'
            WHEN standardized_investor_type IN ('INSURANCE_COMPANY', 'FUND_OF_FUNDS', 'FAMILY_OFFICE')
            THEN 'TIER_2_INSTITUTIONAL'
            ELSE 'OTHER_INVESTOR'
        END as investor_tier,
        
        -- ESG alignment assessment
        CASE 
            WHEN has_esg_requirements = true 
                AND standardized_investor_type IN ('PENSION_FUND', 'ENDOWMENT', 'SOVEREIGN_WEALTH_FUND')
            THEN 'ESG_FOCUSED'
            WHEN has_esg_requirements = true 
            THEN 'ESG_AWARE'
            ELSE 'ESG_NEUTRAL'
        END as esg_alignment,
        
        -- Fundraising priority scoring
        (
            CASE WHEN standardized_investor_type IN ('PENSION_FUND', 'SOVEREIGN_WEALTH_FUND') THEN 3
                 WHEN standardized_investor_type IN ('ENDOWMENT', 'INSURANCE_COMPANY') THEN 2
                 ELSE 1 END +
            CASE WHEN investment_capacity = 'LARGE' THEN 3
                 WHEN investment_capacity = 'MEDIUM' THEN 2
                 ELSE 1 END +
            CASE WHEN risk_tolerance IN ('MODERATE', 'AGGRESSIVE') THEN 2
                 ELSE 1 END
        ) as fundraising_priority_score,
        
        -- Data completeness assessment
        (
            CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN standardized_investor_type IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN standardized_country_code IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN investment_capacity IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN risk_tolerance IS NOT NULL THEN 1 ELSE 0 END
        ) / 5.0 * 100 as overall_completeness_score,
        
        -- Processing metadata
        CURRENT_TIMESTAMP() as processed_at,
        'BRIDGE_TRANSFORMATION' as transformation_type
        
    FROM bridge_mapped
    WHERE id IS NOT NULL  -- Ensure canonical ID exists
)

SELECT * FROM enhanced_mapping