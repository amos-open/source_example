{{
  config(
    materialized='table',
    tags=['intermediate', 'entities']
  )
}}

/*
  Intermediate model for fund entity preparation
  
  This model combines fund data from multiple source systems using cross-reference mappings
  to create unified fund entities ready for canonical model consumption.
  
  Data sources:
  - Fund administration system (primary for financial data)
  - CRM system (supplementary for pipeline data)
  - Cross-reference mappings for entity resolution
  
  Business logic:
  - Prioritize fund admin data for financial attributes
  - Use CRM data to fill gaps in fund information
  - Apply data quality scoring and validation
  - Handle conflicts through source system prioritization
*/

with fund_admin as (
    select * from {{ ref('stg_admin_funds') }}
),

crm_opportunities as (
    select * from {{ ref('stg_crm_opportunities') }}
),

xref_funds as (
    select * from {{ ref('stg_ref_xref_funds') }}
    where recommended_for_resolution = true
),

-- Extract fund information from CRM opportunities
crm_fund_data as (
    select
        company_id as crm_fund_id,
        company_name as fund_name,
        industry_name,
        geography_region,
        min(created_date) as first_opportunity_date,
        count(*) as opportunity_count,
        sum(case when opportunity_status = 'ACTIVE' then 1 else 0 end) as active_opportunities,
        sum(expected_amount) as total_pipeline_amount,
        avg(probability_decimal) as avg_probability
    from crm_opportunities
    where deal_type = 'FUND_INVESTMENT'
        and company_name is not null
    group by company_id, company_name, industry_name, geography_region
),

-- Combine fund data using cross-reference mappings
unified_funds as (
    select
        x.canonical_fund_id,
        
        -- Fund identification (prioritize admin system)
        coalesce(a.fund_name, c.fund_name) as fund_name,
        coalesce(a.fund_legal_name, a.fund_name, c.fund_name) as fund_legal_name,
        a.fund_code as admin_fund_code,
        x.crm_fund_id,
        
        -- Fund characteristics (admin system primary)
        a.vintage_year,
        a.fund_type,
        a.investment_strategy,
        coalesce(a.geography_focus, c.geography_region) as geography_focus,
        coalesce(a.sector_focus, c.industry_name) as sector_focus,
        
        -- Fund sizing (admin system only)
        a.target_size,
        a.target_size_currency,
        a.final_size,
        a.final_size_currency,
        a.validated_base_currency as base_currency_code,
        a.size_variance_percentage,
        
        -- Investment period and lifecycle
        a.investment_period_start,
        a.investment_period_end,
        a.investment_period_years,
        a.fund_life_years,
        a.lifecycle_stage,
        a.fund_age_years,
        
        -- Fee structure (admin system only)
        a.management_fee_rate,
        a.carried_interest_rate,
        a.hurdle_rate,
        a.has_catch_up_provision,
        a.distribution_policy,
        a.management_fee_category,
        a.carry_category,
        
        -- Fund status and dates
        a.fund_status,
        a.first_close_date,
        a.final_close_date,
        a.fundraising_duration_days,
        
        -- CRM pipeline data
        c.first_opportunity_date,
        c.opportunity_count,
        c.active_opportunities,
        c.total_pipeline_amount,
        c.avg_probability,
        
        -- Cross-reference metadata
        x.standardized_confidence as resolution_confidence,
        x.source_systems_count,
        x.resolution_quality,
        x.inferred_fund_strategy,
        x.inferred_fund_number,
        x.inferred_geographic_focus,
        
        -- Data quality and completeness
        a.completeness_score as admin_completeness_score,
        a.data_quality_rating as admin_data_quality,
        x.data_quality_rating as xref_data_quality,
        
        -- Source system tracking
        case 
            when a.fund_code is not null then 'FUND_ADMIN_VENDOR'
            when c.crm_fund_id is not null then 'CRM_VENDOR'
            else 'UNKNOWN'
        end as primary_source_system,
        
        case 
            when a.fund_code is not null and c.crm_fund_id is not null then 'MULTI_SOURCE'
            when a.fund_code is not null then 'ADMIN_ONLY'
            when c.crm_fund_id is not null then 'CRM_ONLY'
            else 'NO_SOURCE'
        end as source_coverage,
        
        -- Audit fields (prioritize admin system)
        coalesce(a.created_date, c.first_opportunity_date) as created_date,
        greatest(
            coalesce(a.last_modified_date, '1900-01-01'::date),
            coalesce(x.last_modified_date, '1900-01-01'::date)
        ) as last_modified_date,
        
        CURRENT_TIMESTAMP() as processed_at

    from xref_funds x
    left join fund_admin a on x.admin_fund_code = a.fund_code
    left join crm_fund_data c on x.crm_fund_id = c.crm_fund_id
),

-- Add derived metrics and validation
enhanced_funds as (
    select
        *,
        
        -- Fund maturity assessment
        case 
            when fund_age_years is null then 'UNKNOWN'
            when fund_age_years < 2 then 'EARLY_STAGE'
            when fund_age_years < 5 then 'GROWTH_STAGE'
            when fund_age_years < 8 then 'MATURE'
            else 'HARVEST'
        end as fund_maturity_stage,
        
        -- Investment capacity assessment
        case 
            when lifecycle_stage = 'INVESTMENT_PERIOD' and target_size > 500 then 'HIGH_CAPACITY'
            when lifecycle_stage = 'INVESTMENT_PERIOD' and target_size > 100 then 'MEDIUM_CAPACITY'
            when lifecycle_stage = 'INVESTMENT_PERIOD' then 'LOW_CAPACITY'
            else 'NO_CAPACITY'
        end as investment_capacity,
        
        -- Performance indicators
        case 
            when fundraising_duration_days is not null then
                case 
                    when fundraising_duration_days <= 365 then 'FAST'
                    when fundraising_duration_days <= 730 then 'NORMAL'
                    else 'SLOW'
                end
            else null
        end as fundraising_speed,
        
        -- Strategy alignment validation
        case 
            when investment_strategy is not null and inferred_fund_strategy is not null then
                case 
                    when upper(investment_strategy) like '%' || inferred_fund_strategy || '%' then 'ALIGNED'
                    else 'MISALIGNED'
                end
            else 'UNKNOWN'
        end as strategy_alignment,
        
        -- Geographic alignment validation
        case 
            when geography_focus is not null and inferred_geographic_focus is not null then
                case 
                    when upper(geography_focus) like '%' || inferred_geographic_focus || '%' then 'ALIGNED'
                    else 'MISALIGNED'
                end
            else 'UNKNOWN'
        end as geography_alignment,
        
        -- Overall data completeness score
        (
            case when fund_name is not null then 1 else 0 end +
            case when vintage_year is not null then 1 else 0 end +
            case when target_size is not null then 1 else 0 end +
            case when base_currency_code is not null then 1 else 0 end +
            case when investment_strategy is not null then 1 else 0 end +
            case when geography_focus is not null then 1 else 0 end +
            case when management_fee_rate is not null then 1 else 0 end +
            case when carried_interest_rate is not null then 1 else 0 end +
            case when first_close_date is not null then 1 else 0 end +
            case when fund_status is not null then 1 else 0 end
        ) / 10.0 * 100 as overall_completeness_score,
        
        -- Data quality assessment
        case 
            when resolution_confidence = 'HIGH' 
                and admin_data_quality = 'HIGH_QUALITY'
                and overall_completeness_score >= 90 
            then 'EXCELLENT'
            when resolution_confidence in ('HIGH', 'MEDIUM')
                and admin_data_quality in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
                and overall_completeness_score >= 70
            then 'GOOD'
            when overall_completeness_score >= 50
            then 'FAIR'
            else 'POOR'
        end as overall_data_quality,
        
        -- Fund attractiveness score for investment decisions
        (
            case when target_size >= 1000 then 3
                 when target_size >= 500 then 2
                 when target_size >= 100 then 1
                 else 0 end +
            case when management_fee_rate <= 0.02 then 2
                 when management_fee_rate <= 0.025 then 1
                 else 0 end +
            case when carried_interest_rate <= 0.20 then 2
                 when carried_interest_rate <= 0.25 then 1
                 else 0 end +
            case when fund_age_years <= 3 then 2
                 when fund_age_years <= 6 then 1
                 else 0 end +
            case when investment_capacity in ('HIGH_CAPACITY', 'MEDIUM_CAPACITY') then 2
                 else 0 end
        ) as fund_attractiveness_score

    from unified_funds
),

final as (
    select
        -- Canonical model format - exact column names and types expected by amos_core
        CAST(canonical_fund_id AS STRING) as id,
        CAST(fund_name AS STRING) as name,
        CAST(fund_type AS STRING) as type,
        CAST(vintage_year AS INT64) as vintage,
        CAST(management_fee_rate AS NUMERIC(7,4)) as management_fee,
        CAST(hurdle_rate AS NUMERIC(7,4)) as hurdle,
        CAST(carried_interest_rate AS NUMERIC(7,4)) as carried_interest,
        CAST(target_size AS NUMERIC(20,2)) as target_commitment,
        CAST(geography_focus AS STRING) as incorporated_in,
        CAST(base_currency_code AS STRING) as base_currency_code,
        CAST(created_date AS STRING) as created_at,
        CAST(last_modified_date AS STRING) as updated_at,
        
        -- Additional intermediate fields for analysis (not used by canonical model)
        fund_legal_name,
        admin_fund_code,
        crm_fund_id,
        investment_strategy,
        sector_focus,
        final_size,
        investment_period_start,
        investment_period_end,
        fund_status,
        first_close_date,
        final_close_date,
        overall_data_quality,
        fund_attractiveness_score,
        
        -- Investment recommendation
        case 
            when fund_attractiveness_score >= 8 and overall_data_quality in ('EXCELLENT', 'GOOD') then 'HIGHLY_RECOMMENDED'
            when fund_attractiveness_score >= 6 and overall_data_quality in ('EXCELLENT', 'GOOD', 'FAIR') then 'RECOMMENDED'
            when fund_attractiveness_score >= 4 then 'CONSIDER'
            else 'NOT_RECOMMENDED'
        end as investment_recommendation,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(canonical_fund_id, fund_name, vintage_year, target_size, final_size, base_currency_code, investment_strategy, geography_focus, management_fee_rate, carried_interest_rate, fund_status, last_modified_date)) as record_hash

    from enhanced_funds
    where canonical_fund_id is not null
)

select * from final