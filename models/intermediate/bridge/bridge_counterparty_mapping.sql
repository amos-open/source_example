{{
  config(
    materialized='view',
    tags=['intermediate', 'bridge', 'counterparty']
  )
}}

/*
  Bridge transformation model for counterparty entity mapping
  
  This model handles column name mismatches between staging accounting counterparties
  and the intermediate counterparty entity model, providing explicit field
  mapping and data type conversions.
  
  Purpose:
  - Map staging column names to intermediate model expectations
  - Handle counterparty type standardization and classification
  - Apply relationship management scoring and categorization
  - Standardize contact information and engagement strategies
  
  Input: stg_acc_counterparties (hypothetical - would need to be created)
  Output: Standardized intermediate counterparty entity structure
*/

-- Note: This assumes we have a staging counterparty model
-- In practice, counterparty data might come from multiple sources
WITH staging_counterparties AS (
    -- This would reference actual staging counterparty model when available
    -- For now, creating a placeholder structure
    SELECT
        'CP001' as counterparty_id,
        'Example Legal Counsel LLP' as counterparty_name,
        'LEGAL_COUNSEL' as counterparty_type,
        'US' as country_code,
        'John Smith' as primary_contact_name,
        'john.smith@example.com' as primary_contact_email,
        'PROFESSIONAL_SERVICES' as counterparty_category,
        'ACTIVE' as relationship_status,
        'STRONG' as relationship_strength,
        DATE('2024-01-15') as last_interaction_date,
        'HIGH_PRIORITY' as relationship_priority,
        'REGULAR_ENGAGEMENT' as engagement_strategy,
        'HIGH' as data_quality_rating,
        TIMESTAMP('2023-01-01 00:00:00') as created_timestamp,
        TIMESTAMP('2024-10-18 00:00:00') as updated_timestamp
    
    -- In real implementation, this would be:
    {# SELECT * FROM {{ ref('stg_acc_counterparties') }} #}
),

-- Apply bridge transformation mapping
bridge_mapped AS (
    SELECT
        {{ alias_staging_columns('counterparty') }}
    FROM staging_counterparties cp
),

-- Add derived fields and relationship management logic
enhanced_mapping AS (
    SELECT
        *,
        
        -- Service provider categorization
        CASE 
            WHEN type IN ('LEGAL_COUNSEL', 'AUDITOR') THEN 'PROFESSIONAL_SERVICES'
            WHEN type IN ('LENDER', 'CO_INVESTOR') THEN 'FINANCIAL_PARTNERS'
            WHEN type IN ('VENDOR', 'SERVICE_PROVIDER') THEN 'OPERATIONAL_VENDORS'
            ELSE 'OTHER'
        END as service_category,
        
        -- Geographic region mapping
        CASE 
            WHEN country_code IN ('US', 'CA') THEN 'NORTH_AMERICA'
            WHEN country_code IN ('GB', 'DE', 'FR', 'NL', 'CH', 'IT', 'ES') THEN 'EUROPE'
            WHEN country_code IN ('JP', 'SG', 'HK', 'AU', 'KR') THEN 'ASIA_PACIFIC'
            WHEN country_code IN ('AE', 'SA', 'QA') THEN 'MIDDLE_EAST'
            ELSE 'OTHER'
        END as geographic_region,
        
        -- Relationship value assessment
        CASE 
            WHEN relationship_strength = 'STRONG' 
                AND relationship_priority = 'HIGH_PRIORITY'
                AND type IN ('LEGAL_COUNSEL', 'AUDITOR', 'CO_INVESTOR')
            THEN 'STRATEGIC_PARTNER'
            WHEN relationship_strength IN ('STRONG', 'MEDIUM')
                AND relationship_priority IN ('HIGH_PRIORITY', 'MEDIUM_PRIORITY')
            THEN 'VALUED_PARTNER'
            WHEN relationship_strength = 'MEDIUM'
                AND relationship_priority = 'MEDIUM_PRIORITY'
            THEN 'STANDARD_PARTNER'
            ELSE 'TRANSACTIONAL_PARTNER'
        END as relationship_value_tier,
        
        -- Engagement frequency recommendation
        CASE 
            WHEN relationship_priority = 'HIGH_PRIORITY' 
                AND relationship_strength = 'STRONG'
            THEN 'MONTHLY'
            WHEN relationship_priority = 'HIGH_PRIORITY'
                OR relationship_strength = 'STRONG'
            THEN 'QUARTERLY'
            WHEN relationship_priority = 'MEDIUM_PRIORITY'
            THEN 'SEMI_ANNUAL'
            ELSE 'ANNUAL'
        END as recommended_engagement_frequency,
        
        -- Contact completeness assessment
        CASE 
            WHEN primary_contact_name IS NOT NULL 
                AND primary_contact_email IS NOT NULL
            THEN 'COMPLETE'
            WHEN primary_contact_name IS NOT NULL 
                OR primary_contact_email IS NOT NULL
            THEN 'PARTIAL'
            ELSE 'INCOMPLETE'
        END as contact_completeness,
        
        -- Relationship freshness assessment
        CASE 
            WHEN last_interaction_date IS NULL THEN 'NO_INTERACTION'
            WHEN DATE_DIFF(CURRENT_DATE(), last_interaction_date, MONTH) <= 3 THEN 'RECENT'
            WHEN DATE_DIFF(CURRENT_DATE(), last_interaction_date, MONTH) <= 12 THEN 'MODERATE'
            ELSE 'STALE'
        END as relationship_freshness,
        
        -- Overall relationship score
        (
            CASE WHEN relationship_strength = 'STRONG' THEN 3
                 WHEN relationship_strength = 'MEDIUM' THEN 2
                 ELSE 1 END +
            CASE WHEN relationship_priority = 'HIGH_PRIORITY' THEN 3
                 WHEN relationship_priority = 'MEDIUM_PRIORITY' THEN 2
                 ELSE 1 END +
            CASE WHEN type IN ('LEGAL_COUNSEL', 'AUDITOR', 'CO_INVESTOR') THEN 2
                 ELSE 1 END +
            CASE WHEN contact_completeness = 'COMPLETE' THEN 2
                 WHEN contact_completeness = 'PARTIAL' THEN 1
                 ELSE 0 END
        ) as overall_relationship_score,
        
        -- Data completeness assessment
        (
            CASE WHEN name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN type IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN country_code IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN primary_contact_name IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN primary_contact_email IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN relationship_status IS NOT NULL THEN 1 ELSE 0 END
        ) / 6.0 * 100 as overall_completeness_score,
        
        -- Processing metadata
        CURRENT_TIMESTAMP() as processed_at,
        'BRIDGE_TRANSFORMATION' as transformation_type
        
    FROM bridge_mapped
    WHERE id IS NOT NULL  -- Ensure ID exists
)

SELECT * FROM enhanced_mapping