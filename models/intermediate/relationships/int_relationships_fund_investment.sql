{{
  config(
    materialized='table',
    tags=['intermediate', 'relationships']
  )
}}

/*
  Intermediate model for fund-investment relationship preparation
  
  This model prepares fund-investment portfolio relationships from portfolio management data,
  combining investment information with performance metrics to create comprehensive
  relationship records for canonical consumption.
  
  Data sources:
  - Portfolio management investments (primary)
  - Company cross-reference mappings for entity resolution
  - Fund cross-reference mappings for entity resolution
  - Investment NAV data for performance metrics
  
  Business logic:
  - Create fund-investment portfolio relationships
  - Calculate investment performance and contribution metrics
  - Assess relationship quality and strategic importance
  - Prepare relationship data for canonical bridge tables
*/

with pm_investments as (
    select * from {{ ref('stg_pm_investments') }}
),

company_xref as (
    select * from {{ ref('stg_ref_xref_companies') }}
    where data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

fund_xref as (
    select * from {{ ref('stg_ref_xref_funds') }}
    where recommended_for_resolution = true
),

admin_nav_investment as (
    select * from {{ ref('stg_admin_nav_investment') }}
),

-- Get latest NAV data for each investment
latest_nav_data as (
    select
        *,
        row_number() over (
            partition by investment_id 
            order by valuation_date desc
        ) as rn
    from admin_nav_investment
    where valuation_date is not null
),

current_nav_data as (
    select * from latest_nav_data where rn = 1
),

-- Prepare fund-investment relationships with entity resolution
fund_investment_relationships as (
    select
        -- Generate relationship identifier
        'REL-FP-' || coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || pi.fund_id) || '-' || coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || pi.company_id) as relationship_id,
        'FUND_INVESTMENT' as relationship_type,
        'PORTFOLIO_MGMT_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || pi.fund_id) as canonical_fund_id,
        coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || pi.company_id) as canonical_company_id,
        pi.fund_id as source_fund_id,
        pi.company_id as source_company_id,
        pi.investment_id as source_investment_id,
        
        -- Fund and company details
        pi.fund_name,
        pi.company_name,
        fx.canonical_fund_name,
        cx.canonical_company_name,
        
        -- Investment details
        pi.investment_date,
        pi.target_exit_date,
        
        -- Investment amounts and structure
        pi.initial_amount_usd,
        pi.total_amount_usd,
        pi.follow_on_amount_usd,
        pi.follow_on_ratio,
        
        -- Investment classification
        pi.standardized_investment_type as investment_type,
        pi.standardized_investment_stage as investment_stage,
        pi.standardized_sector as sector,
        pi.standardized_geography as geography,
        
        -- Ownership and control
        pi.ownership_percentage,
        pi.ownership_category,
        pi.board_seats,
        pi.governance_influence,
        pi.control_classification,
        
        -- Legal terms and protection
        pi.liquidation_preference_type,
        pi.standardized_anti_dilution as anti_dilution_protection,
        pi.has_drag_along_rights,
        pi.has_tag_along_rights,
        pi.protection_level,
        
        -- Strategic information
        pi.investment_thesis,
        pi.key_risks,
        pi.standardized_exit_strategy as exit_strategy,
        
        -- Investment analytics
        pi.investment_age_months,
        pi.investment_age_years,
        pi.target_holding_period_years,
        pi.investment_size_category,
        pi.investment_profile,
        pi.risk_assessment,
        pi.investment_maturity,
        pi.exit_timeline,
        pi.lifecycle_status,
        
        -- Current valuation data (from NAV)
        cnd.cost_basis as current_cost_basis,
        cnd.fair_value as current_fair_value,
        cnd.unrealized_gain_loss as current_unrealized_gain_loss,
        cnd.unrealized_return_multiple as current_return_multiple,
        cnd.valuation_date as latest_valuation_date,
        cnd.standardized_valuation_method as current_valuation_method,
        cnd.performance_category as current_performance_category,
        
        -- Data quality indicators
        pi.completeness_score as investment_data_completeness,
        cnd.completeness_score as nav_data_completeness,
        
        -- Audit fields
        pi.created_date,
        greatest(
            coalesce(pi.last_modified_date, '1900-01-01'::date),
            coalesce(cnd.last_modified_date, '1900-01-01'::date)
        ) as last_modified_date,
        
        CURRENT_TIMESTAMP() as processed_at

    from pm_investments pi
    left join company_xref cx on pi.company_id = cx.pm_company_id
    left join fund_xref fx on pi.fund_id = fx.pm_fund_id
    left join current_nav_data cnd on pi.investment_id = cnd.investment_id
),

-- Add enhanced relationship analytics
enhanced_relationships as (
    select
        *,
        
        -- Portfolio contribution metrics
        case 
            when total_amount_usd >= 100000000 then 'MEGA_POSITION'        -- $100M+
            when total_amount_usd >= 25000000 then 'LARGE_POSITION'        -- $25M-$100M
            when total_amount_usd >= 5000000 then 'MEDIUM_POSITION'        -- $5M-$25M
            when total_amount_usd >= 1000000 then 'SMALL_POSITION'         -- $1M-$5M
            when total_amount_usd > 0 then 'MICRO_POSITION'                -- <$1M
            else 'UNKNOWN_POSITION'
        end as portfolio_position_size,
        
        -- Follow-on investment pattern analysis
        case 
            when follow_on_ratio >= 2.0 then 'HEAVY_FOLLOW_ON'
            when follow_on_ratio >= 1.0 then 'SIGNIFICANT_FOLLOW_ON'
            when follow_on_ratio >= 0.5 then 'MODERATE_FOLLOW_ON'
            when follow_on_ratio > 0 then 'LIGHT_FOLLOW_ON'
            else 'INITIAL_ONLY'
        end as follow_on_pattern,
        
        -- Investment strategy alignment
        case 
            when investment_stage = 'BUYOUT' and ownership_category = 'MAJORITY' then 'CONTROL_STRATEGY'
            when investment_stage = 'BUYOUT' and ownership_category in ('SIGNIFICANT_MINORITY', 'MINORITY') then 'MINORITY_BUYOUT_STRATEGY'
            when investment_stage in ('GROWTH', 'LATE_STAGE') and ownership_category in ('SIGNIFICANT_MINORITY', 'MINORITY') then 'GROWTH_EQUITY_STRATEGY'
            when investment_stage in ('SEED', 'EARLY_STAGE') then 'VENTURE_STRATEGY'
            when investment_stage = 'MEZZANINE' then 'MEZZANINE_STRATEGY'
            when investment_stage = 'DISTRESSED' then 'SPECIAL_SITUATIONS_STRATEGY'
            else 'OTHER_STRATEGY'
        end as strategy_alignment,
        
        -- Performance vs expectations (based on stage and time)
        case 
            when current_return_multiple >= 5.0 and investment_stage = 'SEED' then 'EXCEPTIONAL_PERFORMANCE'
            when current_return_multiple >= 3.0 and investment_stage in ('SEED', 'EARLY_STAGE') then 'EXCEPTIONAL_PERFORMANCE'
            when current_return_multiple >= 2.5 and investment_stage in ('GROWTH', 'BUYOUT') then 'EXCEPTIONAL_PERFORMANCE'
            when current_return_multiple >= 2.0 and investment_stage = 'SEED' then 'STRONG_PERFORMANCE'
            when current_return_multiple >= 1.5 and investment_stage in ('EARLY_STAGE', 'GROWTH') then 'STRONG_PERFORMANCE'
            when current_return_multiple >= 1.3 and investment_stage = 'BUYOUT' then 'STRONG_PERFORMANCE'
            when current_return_multiple >= 1.0 then 'MEETING_EXPECTATIONS'
            when current_return_multiple >= 0.7 then 'BELOW_EXPECTATIONS'
            when current_return_multiple < 0.7 then 'UNDERPERFORMING'
            else 'UNKNOWN_PERFORMANCE'
        end as performance_vs_expectations,
        
        -- Strategic importance assessment
        case 
            when portfolio_position_size in ('MEGA_POSITION', 'LARGE_POSITION') 
                and control_classification in ('FULL_CONTROL', 'OPERATIONAL_CONTROL') then 'FLAGSHIP_INVESTMENT'
            when portfolio_position_size in ('LARGE_POSITION', 'MEDIUM_POSITION') 
                and performance_vs_expectations in ('EXCEPTIONAL_PERFORMANCE', 'STRONG_PERFORMANCE') then 'STAR_INVESTMENT'
            when control_classification in ('FULL_CONTROL', 'OPERATIONAL_CONTROL') then 'CONTROL_INVESTMENT'
            when portfolio_position_size in ('LARGE_POSITION', 'MEDIUM_POSITION') then 'CORE_INVESTMENT'
            when performance_vs_expectations in ('EXCEPTIONAL_PERFORMANCE', 'STRONG_PERFORMANCE') then 'HIGH_PERFORMER'
            else 'STANDARD_INVESTMENT'
        end as strategic_importance,
        
        -- Exit readiness assessment
        case 
            when investment_maturity in ('MATURE_INVESTMENT', 'SEASONED_INVESTMENT') 
                and performance_vs_expectations in ('EXCEPTIONAL_PERFORMANCE', 'STRONG_PERFORMANCE') 
                and exit_timeline in ('NEAR_TERM_EXIT', 'MEDIUM_TERM_EXIT') then 'READY_FOR_EXIT'
            when investment_maturity = 'SEASONED_INVESTMENT' 
                and performance_vs_expectations = 'MEETING_EXPECTATIONS' then 'EXIT_CONSIDERATION'
            when performance_vs_expectations = 'EXCEPTIONAL_PERFORMANCE' 
                and investment_age_years >= 3 then 'EARLY_EXIT_OPPORTUNITY'
            when performance_vs_expectations in ('BELOW_EXPECTATIONS', 'UNDERPERFORMING') 
                and investment_maturity in ('MATURE_INVESTMENT', 'SEASONED_INVESTMENT') then 'PROBLEM_EXIT'
            else 'HOLD_PERIOD'
        end as exit_readiness,
        
        -- Risk assessment based on multiple factors
        case 
            when sector = 'TECHNOLOGY' and investment_stage in ('SEED', 'EARLY_STAGE') then 'HIGH_RISK'
            when sector in ('HEALTHCARE', 'ENERGY') and investment_stage != 'BUYOUT' then 'HIGH_RISK'
            when geography != 'NORTH_AMERICA' and investment_stage in ('SEED', 'EARLY_STAGE') then 'HIGH_RISK'
            when investment_stage = 'DISTRESSED' then 'HIGH_RISK'
            when investment_stage = 'BUYOUT' and control_classification = 'FULL_CONTROL' then 'MEDIUM_RISK'
            when investment_stage in ('GROWTH', 'LATE_STAGE') then 'MEDIUM_RISK'
            when investment_stage = 'MEZZANINE' then 'LOW_RISK'
            else 'UNKNOWN_RISK'
        end as risk_profile,
        
        -- Monitoring priority based on multiple factors
        case 
            when strategic_importance = 'FLAGSHIP_INVESTMENT' then 'CRITICAL_MONITORING'
            when performance_vs_expectations = 'UNDERPERFORMING' 
                and portfolio_position_size in ('MEGA_POSITION', 'LARGE_POSITION') then 'CRITICAL_MONITORING'
            when strategic_importance in ('STAR_INVESTMENT', 'CONTROL_INVESTMENT') then 'HIGH_MONITORING'
            when exit_readiness in ('READY_FOR_EXIT', 'EXIT_CONSIDERATION') then 'HIGH_MONITORING'
            when performance_vs_expectations = 'BELOW_EXPECTATIONS' then 'MEDIUM_MONITORING'
            when strategic_importance = 'CORE_INVESTMENT' then 'MEDIUM_MONITORING'
            else 'STANDARD_MONITORING'
        end as monitoring_priority,
        
        -- Value creation opportunity assessment
        case 
            when control_classification in ('FULL_CONTROL', 'OPERATIONAL_CONTROL') 
                and performance_vs_expectations = 'MEETING_EXPECTATIONS' then 'OPERATIONAL_IMPROVEMENT'
            when governance_influence = 'BOARD_REPRESENTATION' 
                and sector = 'TECHNOLOGY' 
                and investment_stage = 'GROWTH' then 'GROWTH_ACCELERATION'
            when follow_on_pattern in ('LIGHT_FOLLOW_ON', 'INITIAL_ONLY') 
                and performance_vs_expectations = 'STRONG_PERFORMANCE' then 'ADDITIONAL_INVESTMENT'
            when investment_maturity = 'DEVELOPING_INVESTMENT' 
                and performance_vs_expectations = 'EXCEPTIONAL_PERFORMANCE' then 'EARLY_HARVEST'
            when performance_vs_expectations = 'BELOW_EXPECTATIONS' then 'TURNAROUND_REQUIRED'
            else 'MAINTAIN_COURSE'
        end as value_creation_opportunity,
        
        -- Data freshness assessment
        case 
            when latest_valuation_date is not null then
                DATE_DIFF(current_date(), latest_valuation_date, DAY)
            else null
        end as days_since_valuation,
        
        case 
            when days_since_valuation <= 90 then 'CURRENT_VALUATION'
            when days_since_valuation <= 180 then 'RECENT_VALUATION'
            when days_since_valuation <= 365 then 'STALE_VALUATION'
            else 'OUTDATED_VALUATION'
        end as valuation_freshness

    from fund_investment_relationships
),

-- Final relationship preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall relationship value score (0-100)
        (
            case when portfolio_position_size = 'MEGA_POSITION' then 25
                 when portfolio_position_size = 'LARGE_POSITION' then 20
                 when portfolio_position_size = 'MEDIUM_POSITION' then 15
                 when portfolio_position_size = 'SMALL_POSITION' then 10
                 when portfolio_position_size = 'MICRO_POSITION' then 5
                 else 0 end +
            case when performance_vs_expectations = 'EXCEPTIONAL_PERFORMANCE' then 25
                 when performance_vs_expectations = 'STRONG_PERFORMANCE' then 20
                 when performance_vs_expectations = 'MEETING_EXPECTATIONS' then 15
                 when performance_vs_expectations = 'BELOW_EXPECTATIONS' then 5
                 else 0 end +
            case when control_classification = 'FULL_CONTROL' then 20
                 when control_classification = 'OPERATIONAL_CONTROL' then 15
                 when control_classification = 'BOARD_INFLUENCE' then 10
                 when control_classification = 'PASSIVE_INVESTMENT' then 5
                 else 0 end +
            case when strategic_importance in ('FLAGSHIP_INVESTMENT', 'STAR_INVESTMENT') then 15
                 when strategic_importance in ('CONTROL_INVESTMENT', 'CORE_INVESTMENT') then 10
                 when strategic_importance = 'HIGH_PERFORMER' then 8
                 else 5 end +
            case when risk_profile = 'LOW_RISK' then 10
                 when risk_profile = 'MEDIUM_RISK' then 7
                 when risk_profile = 'HIGH_RISK' then 3
                 else 5 end +
            case when valuation_freshness in ('CURRENT_VALUATION', 'RECENT_VALUATION') then 5
                 when valuation_freshness = 'STALE_VALUATION' then 3
                 else 0 end
        ) as relationship_value_score,
        
        -- Portfolio impact assessment
        case 
            when relationship_value_score >= 85 and strategic_importance = 'FLAGSHIP_INVESTMENT' then 'PORTFOLIO_DRIVER'
            when relationship_value_score >= 70 and strategic_importance in ('FLAGSHIP_INVESTMENT', 'STAR_INVESTMENT') then 'KEY_CONTRIBUTOR'
            when relationship_value_score >= 55 and strategic_importance in ('STAR_INVESTMENT', 'CONTROL_INVESTMENT', 'CORE_INVESTMENT') then 'SOLID_CONTRIBUTOR'
            when relationship_value_score >= 40 then 'AVERAGE_CONTRIBUTOR'
            else 'UNDERCONTRIBUTOR'
        end as portfolio_impact,
        
        -- Management attention required
        case 
            when monitoring_priority = 'CRITICAL_MONITORING' then 'DAILY_ATTENTION'
            when monitoring_priority = 'HIGH_MONITORING' then 'WEEKLY_ATTENTION'
            when monitoring_priority = 'MEDIUM_MONITORING' then 'MONTHLY_ATTENTION'
            else 'QUARTERLY_ATTENTION'
        end as management_attention_level,
        
        -- Investment thesis validation
        case 
            when performance_vs_expectations in ('EXCEPTIONAL_PERFORMANCE', 'STRONG_PERFORMANCE') then 'THESIS_VALIDATED'
            when performance_vs_expectations = 'MEETING_EXPECTATIONS' then 'THESIS_ON_TRACK'
            when performance_vs_expectations = 'BELOW_EXPECTATIONS' then 'THESIS_CHALLENGED'
            when performance_vs_expectations = 'UNDERPERFORMING' then 'THESIS_FAILED'
            else 'THESIS_UNKNOWN'
        end as thesis_validation,
        
        -- Data quality assessment
        case 
            when investment_data_completeness >= 80 
                and nav_data_completeness >= 80 
                and valuation_freshness in ('CURRENT_VALUATION', 'RECENT_VALUATION') then 'HIGH_QUALITY'
            when investment_data_completeness >= 60 
                and nav_data_completeness >= 60 then 'MEDIUM_QUALITY'
            when investment_data_completeness >= 40 then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as data_quality_rating,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(relationship_id, canonical_fund_id, canonical_company_id, investment_date, total_amount_usd, ownership_percentage, current_fair_value, current_return_multiple, investment_stage, sector, last_modified_date)) as record_hash

    from enhanced_relationships
)

select * from final