{{
  config(
    materialized='view',
    tags=['intermediate', 'bridge', 'fund']
  )
}}

/*
  Bridge transformation model for fund entity mapping
  
  This model handles column name mismatches between staging admin funds
  and the intermediate fund entity model, providing explicit field
  mapping and data type conversions.
  
  Purpose:
  - Map staging column names to intermediate model expectations
  - Handle data type conversions for numeric fields
  - Apply cross-reference lookups for canonical identifiers
  - Standardize fund characteristics and metadata
  
  Input: stg_admin_funds, stg_ref_xref_funds
  Output: Standardized intermediate fund entity structure
*/

WITH admin_funds AS (
    SELECT * FROM {{ ref('stg_admin_funds') }}
),

xref_funds AS (
    SELECT * FROM {{ ref('stg_ref_xref_funds') }}
    WHERE data_quality_rating IN ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

-- Apply bridge transformation mapping
bridge_mapped AS (
    SELECT
        {{ alias_staging_columns('fund') }}
    FROM admin_funds f
    LEFT JOIN xref_funds x ON x.admin_fund_code = f.fund_code
),

-- Add derived fields and validation
enhanced_mapping AS (
    SELECT
        *,
        
        -- Fund lifecycle assessment
        CASE 
            WHEN vintage IS NULL THEN 'UNKNOWN'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - vintage < 3 THEN 'FUNDRAISING'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - vintage < 8 THEN 'INVESTMENT_PERIOD'
            WHEN EXTRACT(YEAR FROM CURRENT_DATE()) - vintage < 15 THEN 'HARVEST_PERIOD'
            ELSE 'MATURE'
        END as fund_lifecycle_stage,
        
        -- Fee structure validation
        CASE 
            WHEN management_fee IS NOT NULL AND management_fee BETWEEN 0.01 AND 0.05 THEN 'STANDARD'
            WHEN management_fee IS NOT NULL AND management_fee > 0.05 THEN 'HIGH'
            WHEN management_fee IS NOT NULL AND management_fee < 0.01 THEN 'LOW'
            ELSE 'UNKNOWN'
        END as fee_structure_category,
        
        -- Target vs actual size comparison
        CASE 
            WHEN target_commitment IS NOT NULL AND final_size IS NOT NULL THEN
                CASE 
                    WHEN final_size >= target_commitment * 0.9 THEN 'TARGET_ACHIEVED'
                    WHEN final_size >= target_commitment * 0.75 THEN 'PARTIALLY_ACHIEVED'
                    ELSE 'UNDER_TARGET'
                END
            ELSE 'UNKNOWN'
        END as fundraising_success,
        
        -- Data completeness assessment
        (
            CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN type IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN vintage IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN base_currency_code IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN target_commitment IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN management_fee IS NOT NULL THEN 1 ELSE 0 END
        ) / 6.0 * 100 as overall_completeness_score,
        
        -- Investment attractiveness scoring
        (
            CASE WHEN type IN ('Growth Equity', 'Buyout', 'Venture Capital') THEN 2 ELSE 1 END +
            CASE WHEN vintage >= EXTRACT(YEAR FROM CURRENT_DATE()) - 5 THEN 2 ELSE 1 END +
            CASE WHEN management_fee <= 0.025 THEN 2 ELSE 1 END +
            CASE WHEN target_commitment >= 100000000 THEN 2 ELSE 1 END
        ) as investment_attractiveness_score,
        
        -- Processing metadata
        CURRENT_TIMESTAMP() as processed_at,
        'BRIDGE_TRANSFORMATION' as transformation_type
        
    FROM bridge_mapped
    WHERE id IS NOT NULL  -- Ensure canonical ID exists
)

SELECT * FROM enhanced_mapping