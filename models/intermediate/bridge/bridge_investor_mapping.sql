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
    WHERE data_quality_rating IN ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

-- Apply bridge transformation mapping
bridge_mapped AS (
    SELECT
        {{ alias_staging_columns('investor') }}
    FROM admin_investors i
    LEFT JOIN xref_investors x ON x.admin_investor_id = i.investor_id
),

-- Add derived fields and investor profiling
enhanced_mapping AS (
    SELECT
        *,
        
        -- Investor size categorization
        CASE 
            WHEN investment_capacity = 'LARGE' THEN 'INSTITUTIONAL_LARGE'
            WHEN investment_capacity = 'MEDIUM' THEN 'INSTITUTIONAL_MEDIUM'
            WHEN investment_capacity = 'SMALL' THEN 'INSTITUTIONAL_SMALL'
            WHEN standardized_investor_type = 'HIGH_NET_WORTH' THEN 'PRIVATE_WEALTH'
            ELSE 'OTHER'
        END as investor_size_category,
        
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
                AND investment_capacity = 'LARGE' 
                AND compliance_status = 'FULLY_COMPLIANT' 
            THEN 'TIER_1_INSTITUTIONAL'
            WHEN standardized_investor_type IN ('INSURANCE_COMPANY', 'FUND_OF_FUNDS', 'FAMILY_OFFICE')
                AND investment_capacity IN ('LARGE', 'MEDIUM')
                AND compliance_status IN ('FULLY_COMPLIANT', 'PARTIALLY_COMPLIANT')
            THEN 'TIER_2_INSTITUTIONAL'
            WHEN standardized_investor_type = 'HIGH_NET_WORTH'
                AND compliance_status = 'FULLY_COMPLIANT'
            THEN 'QUALIFIED_PRIVATE'
            ELSE 'STANDARD_INVESTOR'
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
            CASE WHEN compliance_status = 'FULLY_COMPLIANT' THEN 2
                 WHEN compliance_status = 'PARTIALLY_COMPLIANT' THEN 1
                 ELSE 0 END +
            CASE WHEN risk_tolerance IN ('MODERATE', 'AGGRESSIVE') THEN 2
                 ELSE 1 END
        ) as fundraising_priority_score,
        
        -- Data completeness assessment
        (
            CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN standardized_investor_type IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN standardized_country_code IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN investment_capacity IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN risk_tolerance IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN compliance_status IS NOT NULL THEN 1 ELSE 0 END
        ) / 6.0 * 100 as overall_completeness_score,
        
        -- Processing metadata
        CURRENT_TIMESTAMP() as processed_at,
        'BRIDGE_TRANSFORMATION' as transformation_type
        
    FROM bridge_mapped
    WHERE id IS NOT NULL  -- Ensure canonical ID exists
)

SELECT * FROM enhanced_mapping