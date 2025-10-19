{{
  config(
    materialized='table',
    tags=['intermediate', 'entities']
  )
}}

/*
  Intermediate model for investor entity preparation
  
  This model prepares investor master data from the fund administration system,
  enriching it with derived metrics and comprehensive categorization for
  fundraising and relationship management purposes.
  
  Data sources:
  - Fund administration system (primary source for investor data)
  - Cross-reference mappings (if available for multi-system environments)
  
  Business logic:
  - Standardize investor classifications and geographic regions
  - Calculate investor attractiveness and capacity scores
  - Assess compliance status and fundraising eligibility
  - Generate comprehensive investor profiling and segmentation
*/

with admin_investors as (
    select * from {{ ref('stg_admin_investors') }}
),

-- Calculate investor commitment and activity metrics
investor_activity as (
    select
        i.investor_code,
        
        -- Basic investor information
        i.investor_name,
        i.investor_legal_name,
        i.standardized_investor_type,
        i.investor_size_category,
        i.standardized_country_code,
        i.geographic_region,
        
        -- Contact and compliance
        i.contact_person_name,
        i.contact_email,
        i.contact_phone,
        i.compliance_status,
        i.kyc_status,
        i.aml_status,
        i.accredited_status,
        
        -- Investment characteristics
        i.investment_capacity,
        i.risk_tolerance,
        i.liquidity_preference,
        i.has_esg_requirements,
        
        -- Scoring
        i.capacity_score,
        i.risk_score,
        i.liquidity_score,
        i.investor_attractiveness_score,
        i.fundraising_status,
        
        -- Data quality
        i.completeness_score,
        i.data_quality_rating,
        
        -- Audit fields
        i.created_date,
        i.last_modified_date,
        i.source_system,
        i.loaded_at,
        
        CURRENT_TIMESTAMP() as processed_at

    from admin_investors i
),

-- Add enhanced investor profiling and segmentation
enhanced_investors as (
    select
        *,
        
        -- Investor tier classification
        case 
            when standardized_investor_type in ('PENSION_FUND', 'SOVEREIGN_WEALTH_FUND', 'INSURANCE_COMPANY')
                and investment_capacity = 'HIGH' then 'TIER_1_INSTITUTIONAL'
            when standardized_investor_type in ('ENDOWMENT', 'FOUNDATION', 'FUND_OF_FUNDS')
                and investment_capacity in ('HIGH', 'MEDIUM') then 'TIER_2_INSTITUTIONAL'
            when standardized_investor_type = 'FAMILY_OFFICE'
                and investment_capacity in ('HIGH', 'MEDIUM') then 'TIER_1_PRIVATE_WEALTH'
            when standardized_investor_type = 'HIGH_NET_WORTH'
                and investment_capacity = 'HIGH' then 'TIER_2_PRIVATE_WEALTH'
            when standardized_investor_type in ('BANK', 'CORPORATE')
                and investment_capacity in ('HIGH', 'MEDIUM') then 'CORPORATE_STRATEGIC'
            else 'OTHER'
        end as investor_tier,
        
        -- Investment behavior profile
        case 
            when risk_tolerance = 'HIGH' and liquidity_preference = 'LOW' then 'AGGRESSIVE_LONG_TERM'
            when risk_tolerance = 'MEDIUM' and liquidity_preference = 'LOW' then 'BALANCED_LONG_TERM'
            when risk_tolerance = 'LOW' and liquidity_preference = 'LOW' then 'CONSERVATIVE_LONG_TERM'
            when risk_tolerance = 'HIGH' and liquidity_preference = 'MEDIUM' then 'AGGRESSIVE_BALANCED'
            when risk_tolerance = 'MEDIUM' and liquidity_preference = 'MEDIUM' then 'BALANCED'
            when risk_tolerance = 'LOW' and liquidity_preference = 'MEDIUM' then 'CONSERVATIVE_BALANCED'
            when liquidity_preference = 'HIGH' then 'LIQUIDITY_FOCUSED'
            else 'UNDEFINED'
        end as investment_behavior_profile,
        
        -- ESG alignment assessment
        case 
            when has_esg_requirements = true then 'ESG_REQUIRED'
            when has_esg_requirements = false then 'ESG_NEUTRAL'
            else 'ESG_UNKNOWN'
        end as esg_alignment,
        
        -- Regulatory complexity assessment
        case 
            when standardized_country_code in ('US', 'GB', 'DE', 'FR', 'JP') 
                and standardized_investor_type in ('PENSION_FUND', 'INSURANCE_COMPANY', 'SOVEREIGN_WEALTH_FUND') 
            then 'HIGH_COMPLEXITY'
            when standardized_country_code in ('US', 'GB', 'DE', 'FR', 'JP', 'CA', 'AU', 'CH', 'NL') 
                and standardized_investor_type in ('ENDOWMENT', 'FOUNDATION', 'FUND_OF_FUNDS') 
            then 'MEDIUM_COMPLEXITY'
            when standardized_investor_type in ('FAMILY_OFFICE', 'HIGH_NET_WORTH') 
            then 'LOW_COMPLEXITY'
            else 'UNKNOWN_COMPLEXITY'
        end as regulatory_complexity,
        
        -- Fundraising priority scoring
        case 
            when investor_attractiveness_score >= 2.5 
                and compliance_status = 'FULLY_COMPLIANT'
                and investor_tier like 'TIER_1%' 
            then 'PRIORITY_1'
            when investor_attractiveness_score >= 2.0 
                and compliance_status in ('FULLY_COMPLIANT', 'PARTIALLY_COMPLIANT')
                and investor_tier in ('TIER_1_INSTITUTIONAL', 'TIER_2_INSTITUTIONAL', 'TIER_1_PRIVATE_WEALTH') 
            then 'PRIORITY_2'
            when investor_attractiveness_score >= 1.5 
                and compliance_status != 'NON_COMPLIANT'
            then 'PRIORITY_3'
            else 'LOW_PRIORITY'
        end as fundraising_priority,
        
        -- Relationship management complexity
        case 
            when regulatory_complexity = 'HIGH_COMPLEXITY' 
                and has_esg_requirements = true 
            then 'COMPLEX'
            when regulatory_complexity in ('HIGH_COMPLEXITY', 'MEDIUM_COMPLEXITY') 
                or has_esg_requirements = true 
            then 'MODERATE'
            else 'SIMPLE'
        end as relationship_complexity,
        
        -- Investment ticket size estimation (based on investor type and capacity)
        case 
            when standardized_investor_type = 'SOVEREIGN_WEALTH_FUND' and investment_capacity = 'HIGH' then 'VERY_LARGE'  -- $50M+
            when standardized_investor_type in ('PENSION_FUND', 'INSURANCE_COMPANY') and investment_capacity = 'HIGH' then 'LARGE'  -- $25-50M
            when standardized_investor_type in ('ENDOWMENT', 'FOUNDATION') and investment_capacity = 'HIGH' then 'MEDIUM_LARGE'  -- $10-25M
            when standardized_investor_type = 'FUND_OF_FUNDS' and investment_capacity in ('HIGH', 'MEDIUM') then 'MEDIUM'  -- $5-15M
            when standardized_investor_type = 'FAMILY_OFFICE' and investment_capacity = 'HIGH' then 'MEDIUM'  -- $5-15M
            when standardized_investor_type = 'FAMILY_OFFICE' and investment_capacity = 'MEDIUM' then 'SMALL_MEDIUM'  -- $2-8M
            when standardized_investor_type = 'HIGH_NET_WORTH' and investment_capacity = 'HIGH' then 'SMALL_MEDIUM'  -- $2-8M
            when investment_capacity = 'MEDIUM' then 'SMALL'  -- $1-5M
            when investment_capacity = 'LOW' then 'VERY_SMALL'  -- <$2M
            else 'UNKNOWN'
        end as expected_ticket_size,
        
        -- Due diligence requirements assessment
        case 
            when standardized_investor_type in ('SOVEREIGN_WEALTH_FUND', 'PENSION_FUND', 'INSURANCE_COMPANY') then 'EXTENSIVE'
            when standardized_investor_type in ('ENDOWMENT', 'FOUNDATION', 'FUND_OF_FUNDS') then 'COMPREHENSIVE'
            when standardized_investor_type in ('FAMILY_OFFICE', 'BANK', 'CORPORATE') then 'STANDARD'
            when standardized_investor_type = 'HIGH_NET_WORTH' then 'BASIC'
            else 'UNKNOWN'
        end as due_diligence_requirements,
        
        -- Decision timeline estimation
        case 
            when standardized_investor_type in ('SOVEREIGN_WEALTH_FUND', 'PENSION_FUND') then 'VERY_LONG'  -- 12+ months
            when standardized_investor_type in ('INSURANCE_COMPANY', 'ENDOWMENT', 'FOUNDATION') then 'LONG'  -- 6-12 months
            when standardized_investor_type in ('FUND_OF_FUNDS', 'FAMILY_OFFICE') then 'MEDIUM'  -- 3-6 months
            when standardized_investor_type in ('BANK', 'CORPORATE', 'HIGH_NET_WORTH') then 'SHORT'  -- 1-3 months
            else 'UNKNOWN'
        end as expected_decision_timeline,
        
        -- Overall investor score for prioritization
        (
            investor_attractiveness_score * 0.4 +
            case when compliance_status = 'FULLY_COMPLIANT' then 3
                 when compliance_status = 'PARTIALLY_COMPLIANT' then 2
                 else 0 end * 0.3 +
            case when investor_tier like 'TIER_1%' then 3
                 when investor_tier like 'TIER_2%' then 2
                 else 1 end * 0.2 +
            case when data_quality_rating = 'HIGH' then 3
                 when data_quality_rating = 'MEDIUM' then 2
                 else 1 end * 0.1
        ) as overall_investor_score

    from investor_activity
),

-- Apply bridge transformation for consistent column mapping
bridge_transformed as (
    select * from {{ ref('bridge_investor_mapping') }}
),

final as (
    select
        -- Canonical model format using bridge transformation
        bt.id,
        bt.name,
        bt.investor_type_id,
        bt.created_at,
        bt.updated_at,
        
        -- Additional intermediate fields for analysis (not used by canonical model)
        bt.admin_investor_code as investor_code,
        bt.standardized_investor_type,
        bt.standardized_country_code,
        null as investment_capacity,
        null as risk_tolerance,
        bt.compliance_status,
        bt.has_esg_requirements,
        
        -- Final investor classification for fundraising (using fundraising_priority_score instead of overall_investor_score)
        case 
            when bt.fundraising_priority_score >= 7 then 'TARGET_INVESTOR'
            when bt.fundraising_priority_score >= 5 then 'QUALIFIED_INVESTOR'
            when bt.fundraising_priority_score >= 3 then 'POTENTIAL_INVESTOR'
            when bt.compliance_status != 'NON_COMPLIANT' then 'PROSPECT_INVESTOR'
            else 'EXCLUDED_INVESTOR'
        end as final_investor_classification,
        
        -- Engagement strategy recommendation
        case 
            when final_investor_classification = 'TARGET_INVESTOR' then 'DIRECT_SENIOR_ENGAGEMENT'
            when final_investor_classification = 'QUALIFIED_INVESTOR' then 'STRUCTURED_ENGAGEMENT'
            when final_investor_classification = 'POTENTIAL_INVESTOR' then 'NURTURE_RELATIONSHIP'
            when final_investor_classification = 'PROSPECT_INVESTOR' then 'MONITOR_AND_QUALIFY'
            else 'NO_ENGAGEMENT'
        end as engagement_strategy,
        
        -- Record hash for change detection
        HASH(bt.admin_investor_code, bt.name, bt.standardized_investor_type, bt.standardized_country_code, bt.compliance_status, bt.has_esg_requirements, bt.updated_at) as record_hash

    from bridge_transformed bt
    where bt.id is not null
)

select * from final