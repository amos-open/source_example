{{
  config(
    materialized='table',
    tags=['intermediate', 'snapshots']
  )
}}

/*
  Intermediate model for investment-level NAV snapshot preparation
  
  This model prepares investment-level NAV calculations and valuations,
  combining data from fund administration systems with entity resolution
  to create comprehensive investment performance snapshots.
  
  Data sources:
  - Fund administration investment NAV data (primary)
  - Company cross-reference mappings for entity resolution
  - Fund cross-reference mappings for entity resolution
  - Currency exchange rates for conversion
  
  Business logic:
  - Standardize investment valuations and performance metrics
  - Apply currency conversion to base currency
  - Calculate derived performance indicators and analytics
  - Prepare investment valuation data for canonical consumption
*/

with admin_nav_investment as (
    select * from {{ ref('stg_admin_nav_investment') }}
),

company_xref as (
    select * from {{ ref('stg_ref_xref_companies') }}
    where data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

fund_xref as (
    select * from {{ ref('stg_ref_xref_funds') }}
    where recommended_for_resolution = true
),

fx_rates as (
    select * from {{ ref('stg_ref_fx_rates') }}
),

-- Get latest exchange rates for currency conversion
latest_fx_rates as (
    select
        from_currency,
        to_currency,
        exchange_rate,
        rate_date,
        row_number() over (
            partition by from_currency, to_currency 
            order by rate_date desc
        ) as rn
    from fx_rates
    where rate_date <= current_date()
),

current_fx_rates as (
    select * from latest_fx_rates where rn = 1
),

-- Prepare investment NAV snapshots with entity resolution
investment_nav_snapshots as (
    select
        ani.nav_investment_id as snapshot_id,
        'INVESTMENT_NAV' as snapshot_type,
        'FUND_ADMIN_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || ani.investment_id) as canonical_company_id,
        coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || ani.fund_code) as canonical_fund_id,
        ani.investment_id as source_investment_id,
        ani.fund_code as source_fund_id,
        
        -- Investment details
        ani.investment_name,
        cx.canonical_company_name as company_name,
        fx.canonical_fund_name as fund_name,
        
        -- Snapshot date
        ani.valuation_date as snapshot_date,
        ani.valuation_quarter,
        ani.valuation_year,
        
        -- Financial metrics in original currency
        ani.cost_basis,
        ani.validated_cost_currency as cost_currency,
        ani.fair_value,
        ani.validated_fair_value_currency as fair_value_currency,
        ani.unrealized_gain_loss,
        
        -- Valuation methodology
        ani.standardized_valuation_method as valuation_method,
        ani.valuation_multiple,
        ani.last_financing_valuation,
        
        -- Investment characteristics
        ani.investment_date,
        ani.standardized_investment_stage as investment_stage,
        ani.standardized_sector as sector,
        ani.standardized_geography as geography,
        
        -- Ownership and governance
        ani.ownership_percentage,
        ani.ownership_category,
        ani.board_seats,
        ani.governance_influence,
        ani.liquidation_preference,
        
        -- Performance metrics
        ani.unrealized_return_multiple,
        ani.unrealized_return_percentage,
        ani.performance_category,
        
        -- Investment analytics
        ani.investment_age_months,
        ani.investment_age_years,
        
        -- Data quality indicators
        ani.unrealized_calculation_variance,
        ani.investment_quality_rating,
        ani.completeness_score,
        
        -- Audit fields
        ani.created_date,
        ani.last_modified_date,
        current_timestamp() as processed_at

    from admin_nav_investment ani
    left join company_xref cx on ani.investment_id = cx.pm_company_id
    left join fund_xref fx on ani.fund_code = fx.admin_fund_code
),

-- Apply currency conversion to USD base currency
currency_converted_snapshots as (
    select
        *,
        
        -- Convert cost basis to USD
        case 
            when cost_currency = 'USD' then cost_basis
            when fx_cost.exchange_rate is not null then cost_basis * fx_cost.exchange_rate
            else cost_basis  -- Keep original if no rate available
        end as cost_basis_usd,
        
        -- Convert fair value to USD
        case 
            when fair_value_currency = 'USD' then fair_value
            when fx_fair.exchange_rate is not null then fair_value * fx_fair.exchange_rate
            else fair_value  -- Keep original if no rate available
        end as fair_value_usd,
        
        -- Convert unrealized gain/loss to USD
        case 
            when fair_value_currency = 'USD' then unrealized_gain_loss
            when fx_fair.exchange_rate is not null then unrealized_gain_loss * fx_fair.exchange_rate
            else unrealized_gain_loss
        end as unrealized_gain_loss_usd,
        
        -- Convert last financing valuation to USD
        case 
            when fair_value_currency = 'USD' then last_financing_valuation
            when fx_fair.exchange_rate is not null then last_financing_valuation * fx_fair.exchange_rate
            else last_financing_valuation
        end as last_financing_valuation_usd,
        
        -- Currency conversion metadata
        fx_cost.exchange_rate as cost_fx_rate,
        fx_fair.exchange_rate as fair_value_fx_rate,
        case 
            when (cost_currency != 'USD' and fx_cost.exchange_rate is not null) 
                or (fair_value_currency != 'USD' and fx_fair.exchange_rate is not null) then true
            else false
        end as currency_converted

    from investment_nav_snapshots ins
    left join current_fx_rates fx_cost 
        on ins.cost_currency = fx_cost.from_currency 
        and fx_cost.to_currency = 'USD'
    left join current_fx_rates fx_fair 
        on ins.fair_value_currency = fx_fair.from_currency 
        and fx_fair.to_currency = 'USD'
),

-- Add enhanced analytics and performance assessment
enhanced_snapshots as (
    select
        *,
        
        -- Investment size categorization (in USD, based on cost basis)
        case 
            when cost_basis_usd >= 100000000 then 'MEGA_INVESTMENT'    -- $100M+
            when cost_basis_usd >= 25000000 then 'LARGE_INVESTMENT'    -- $25M-$100M
            when cost_basis_usd >= 5000000 then 'MEDIUM_INVESTMENT'    -- $5M-$25M
            when cost_basis_usd >= 1000000 then 'SMALL_INVESTMENT'     -- $1M-$5M
            when cost_basis_usd > 0 then 'MICRO_INVESTMENT'            -- <$1M
            else 'UNKNOWN'
        end as investment_size_category,
        
        -- Recalculate performance metrics in USD
        case 
            when cost_basis_usd is not null and cost_basis_usd > 0 and fair_value_usd is not null then
                (fair_value_usd / cost_basis_usd) - 1
            else null
        end as unrealized_return_multiple_usd,
        
        case 
            when cost_basis_usd is not null and cost_basis_usd > 0 and unrealized_gain_loss_usd is not null then
                (unrealized_gain_loss_usd / cost_basis_usd) * 100
            else null
        end as unrealized_return_percentage_usd,
        
        -- Valuation method reliability assessment
        case 
            when valuation_method = 'MARKET_MULTIPLE' then 'HIGH_RELIABILITY'
            when valuation_method = 'RECENT_TRANSACTION' then 'HIGH_RELIABILITY'
            when valuation_method = 'DCF' then 'MEDIUM_RELIABILITY'
            when valuation_method = 'COST_BASIS' then 'LOW_RELIABILITY'
            when valuation_method = 'LIQUIDATION_VALUE' then 'LOW_RELIABILITY'
            else 'UNKNOWN_RELIABILITY'
        end as valuation_reliability,
        
        -- Investment maturity assessment
        case 
            when investment_age_years >= 7 then 'MATURE_INVESTMENT'
            when investment_age_years >= 4 then 'SEASONED_INVESTMENT'
            when investment_age_years >= 2 then 'DEVELOPING_INVESTMENT'
            when investment_age_years >= 1 then 'EARLY_INVESTMENT'
            else 'NEW_INVESTMENT'
        end as investment_maturity,
        
        -- Performance vs stage expectations
        case 
            when investment_stage = 'SEED' and unrealized_return_multiple_usd >= 5.0 then 'EXCEEDING_EXPECTATIONS'
            when investment_stage = 'SEED' and unrealized_return_multiple_usd >= 2.0 then 'MEETING_EXPECTATIONS'
            when investment_stage = 'EARLY_STAGE' and unrealized_return_multiple_usd >= 4.0 then 'EXCEEDING_EXPECTATIONS'
            when investment_stage = 'EARLY_STAGE' and unrealized_return_multiple_usd >= 2.0 then 'MEETING_EXPECTATIONS'
            when investment_stage = 'GROWTH' and unrealized_return_multiple_usd >= 3.0 then 'EXCEEDING_EXPECTATIONS'
            when investment_stage = 'GROWTH' and unrealized_return_multiple_usd >= 1.5 then 'MEETING_EXPECTATIONS'
            when investment_stage = 'BUYOUT' and unrealized_return_multiple_usd >= 2.5 then 'EXCEEDING_EXPECTATIONS'
            when investment_stage = 'BUYOUT' and unrealized_return_multiple_usd >= 1.5 then 'MEETING_EXPECTATIONS'
            when unrealized_return_multiple_usd >= 1.0 then 'BELOW_EXPECTATIONS'
            when unrealized_return_multiple_usd < 1.0 then 'UNDERPERFORMING'
            else 'UNKNOWN_PERFORMANCE'
        end as performance_vs_expectations,
        
        -- Sector performance context
        case 
            when sector = 'TECHNOLOGY' and unrealized_return_multiple_usd >= 3.0 then 'STRONG_SECTOR_PERFORMANCE'
            when sector = 'TECHNOLOGY' and unrealized_return_multiple_usd >= 1.5 then 'AVERAGE_SECTOR_PERFORMANCE'
            when sector = 'HEALTHCARE' and unrealized_return_multiple_usd >= 2.5 then 'STRONG_SECTOR_PERFORMANCE'
            when sector = 'HEALTHCARE' and unrealized_return_multiple_usd >= 1.3 then 'AVERAGE_SECTOR_PERFORMANCE'
            when sector in ('CONSUMER', 'INDUSTRIALS') and unrealized_return_multiple_usd >= 2.0 then 'STRONG_SECTOR_PERFORMANCE'
            when sector in ('CONSUMER', 'INDUSTRIALS') and unrealized_return_multiple_usd >= 1.2 then 'AVERAGE_SECTOR_PERFORMANCE'
            when unrealized_return_multiple_usd >= 1.0 then 'WEAK_SECTOR_PERFORMANCE'
            else 'POOR_SECTOR_PERFORMANCE'
        end as sector_performance_context,
        
        -- Geographic risk-adjusted performance
        case 
            when geography = 'NORTH_AMERICA' and unrealized_return_multiple_usd >= 2.0 then 'STRONG_GEO_PERFORMANCE'
            when geography = 'NORTH_AMERICA' and unrealized_return_multiple_usd >= 1.3 then 'AVERAGE_GEO_PERFORMANCE'
            when geography = 'EUROPE' and unrealized_return_multiple_usd >= 1.8 then 'STRONG_GEO_PERFORMANCE'
            when geography = 'EUROPE' and unrealized_return_multiple_usd >= 1.2 then 'AVERAGE_GEO_PERFORMANCE'
            when geography = 'ASIA_PACIFIC' and unrealized_return_multiple_usd >= 2.2 then 'STRONG_GEO_PERFORMANCE'
            when geography = 'ASIA_PACIFIC' and unrealized_return_multiple_usd >= 1.4 then 'AVERAGE_GEO_PERFORMANCE'
            when unrealized_return_multiple_usd >= 1.0 then 'WEAK_GEO_PERFORMANCE'
            else 'POOR_GEO_PERFORMANCE'
        end as geographic_performance_context,
        
        -- Control premium assessment
        case 
            when ownership_category = 'MAJORITY' and governance_influence = 'STRONG_GOVERNANCE' then 'FULL_CONTROL_PREMIUM'
            when ownership_category in ('SIGNIFICANT_MINORITY', 'MAJORITY') and governance_influence in ('STRONG_GOVERNANCE', 'BOARD_REPRESENTATION') then 'CONTROL_INFLUENCE_PREMIUM'
            when ownership_category in ('MINORITY', 'SIGNIFICANT_MINORITY') and governance_influence = 'BOARD_REPRESENTATION' then 'BOARD_INFLUENCE_PREMIUM'
            when ownership_category in ('MINORITY', 'SMALL_STAKE') then 'MINORITY_DISCOUNT'
            else 'UNKNOWN_CONTROL_IMPACT'
        end as control_premium_assessment,
        
        -- Valuation multiple analysis
        case 
            when valuation_multiple >= 20 then 'HIGH_MULTIPLE'
            when valuation_multiple >= 10 then 'MEDIUM_HIGH_MULTIPLE'
            when valuation_multiple >= 5 then 'MEDIUM_MULTIPLE'
            when valuation_multiple >= 2 then 'LOW_MEDIUM_MULTIPLE'
            when valuation_multiple > 0 then 'LOW_MULTIPLE'
            else 'NO_MULTIPLE_DATA'
        end as valuation_multiple_category,
        
        -- Snapshot recency assessment
        case 
            when snapshot_date is not null then
                datediff('day', snapshot_date, current_date())
            else null
        end as days_since_snapshot,
        
        case 
            when days_since_snapshot <= 30 then 'CURRENT'
            when days_since_snapshot <= 90 then 'RECENT'
            when days_since_snapshot <= 180 then 'STALE'
            else 'OUTDATED'
        end as snapshot_freshness,
        
        -- Mark-to-market vs cost comparison
        case 
            when fair_value_usd > cost_basis_usd * 2 then 'SIGNIFICANT_APPRECIATION'
            when fair_value_usd > cost_basis_usd * 1.5 then 'MODERATE_APPRECIATION'
            when fair_value_usd > cost_basis_usd * 1.1 then 'SLIGHT_APPRECIATION'
            when fair_value_usd >= cost_basis_usd * 0.9 then 'STABLE_VALUE'
            when fair_value_usd >= cost_basis_usd * 0.7 then 'MODERATE_DECLINE'
            when fair_value_usd < cost_basis_usd * 0.7 then 'SIGNIFICANT_DECLINE'
            else 'UNKNOWN_VALUE_CHANGE'
        end as value_change_assessment

    from currency_converted_snapshots
),

-- Final snapshot preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall investment quality score (0-100)
        (
            case when performance_vs_expectations = 'EXCEEDING_EXPECTATIONS' then 25
                 when performance_vs_expectations = 'MEETING_EXPECTATIONS' then 20
                 when performance_vs_expectations = 'BELOW_EXPECTATIONS' then 10
                 when performance_vs_expectations = 'UNDERPERFORMING' then 0
                 else 5 end +
            case when valuation_reliability = 'HIGH_RELIABILITY' then 20
                 when valuation_reliability = 'MEDIUM_RELIABILITY' then 15
                 when valuation_reliability = 'LOW_RELIABILITY' then 5
                 else 0 end +
            case when control_premium_assessment in ('FULL_CONTROL_PREMIUM', 'CONTROL_INFLUENCE_PREMIUM') then 15
                 when control_premium_assessment = 'BOARD_INFLUENCE_PREMIUM' then 10
                 when control_premium_assessment = 'MINORITY_DISCOUNT' then 5
                 else 0 end +
            case when sector_performance_context = 'STRONG_SECTOR_PERFORMANCE' then 15
                 when sector_performance_context = 'AVERAGE_SECTOR_PERFORMANCE' then 10
                 when sector_performance_context = 'WEAK_SECTOR_PERFORMANCE' then 5
                 else 0 end +
            case when geographic_performance_context = 'STRONG_GEO_PERFORMANCE' then 10
                 when geographic_performance_context = 'AVERAGE_GEO_PERFORMANCE' then 7
                 when geographic_performance_context = 'WEAK_GEO_PERFORMANCE' then 3
                 else 0 end +
            case when investment_quality_rating = 'HIGH_QUALITY' then 10
                 when investment_quality_rating = 'MEDIUM_QUALITY' then 7
                 else 3 end +
            case when snapshot_freshness in ('CURRENT', 'RECENT') then 5
                 when snapshot_freshness = 'STALE' then 3
                 else 0 end
        ) as investment_quality_score,
        
        -- Portfolio monitoring priority
        case 
            when performance_vs_expectations = 'UNDERPERFORMING' and investment_maturity in ('MATURE_INVESTMENT', 'SEASONED_INVESTMENT') then 'HIGH_PRIORITY'
            when value_change_assessment = 'SIGNIFICANT_DECLINE' then 'HIGH_PRIORITY'
            when investment_size_category in ('MEGA_INVESTMENT', 'LARGE_INVESTMENT') and performance_vs_expectations = 'BELOW_EXPECTATIONS' then 'HIGH_PRIORITY'
            when control_premium_assessment in ('FULL_CONTROL_PREMIUM', 'CONTROL_INFLUENCE_PREMIUM') then 'MEDIUM_PRIORITY'
            when investment_size_category in ('MEGA_INVESTMENT', 'LARGE_INVESTMENT') then 'MEDIUM_PRIORITY'
            when snapshot_freshness in ('STALE', 'OUTDATED') then 'MEDIUM_PRIORITY'
            else 'LOW_PRIORITY'
        end as monitoring_priority,
        
        -- Exit readiness assessment
        case 
            when investment_maturity in ('MATURE_INVESTMENT', 'SEASONED_INVESTMENT') 
                and performance_vs_expectations in ('EXCEEDING_EXPECTATIONS', 'MEETING_EXPECTATIONS') then 'EXIT_READY'
            when investment_maturity = 'SEASONED_INVESTMENT' 
                and value_change_assessment in ('SIGNIFICANT_APPRECIATION', 'MODERATE_APPRECIATION') then 'EXIT_CONSIDERATION'
            when investment_maturity in ('DEVELOPING_INVESTMENT', 'EARLY_INVESTMENT') 
                and performance_vs_expectations = 'EXCEEDING_EXPECTATIONS' then 'EARLY_EXIT_OPPORTUNITY'
            when performance_vs_expectations = 'UNDERPERFORMING' then 'EXIT_CHALLENGE'
            else 'HOLD_PERIOD'
        end as exit_readiness,
        
        -- Data quality flags
        case 
            when currency_converted and (cost_fx_rate is null or fair_value_fx_rate is null) then 'FX_RATE_MISSING'
            when unrealized_calculation_variance = true then 'CALCULATION_VARIANCE'
            when completeness_score < 70 then 'INCOMPLETE_DATA'
            when snapshot_freshness = 'OUTDATED' then 'STALE_DATA'
            when valuation_reliability = 'UNKNOWN_RELIABILITY' then 'VALUATION_METHOD_UNCLEAR'
            else 'NO_ISSUES'
        end as data_quality_flag,
        
        -- Investment recommendation
        case 
            when investment_quality_score >= 85 and performance_vs_expectations = 'EXCEEDING_EXPECTATIONS' then 'STAR_PERFORMER'
            when investment_quality_score >= 70 and performance_vs_expectations in ('EXCEEDING_EXPECTATIONS', 'MEETING_EXPECTATIONS') then 'STRONG_PERFORMER'
            when investment_quality_score >= 55 and performance_vs_expectations = 'MEETING_EXPECTATIONS' then 'SOLID_PERFORMER'
            when investment_quality_score >= 40 then 'MONITOR_CLOSELY'
            else 'UNDERPERFORMER'
        end as investment_classification,
        
        -- Record hash for change detection
        hash(
            snapshot_id,
            canonical_company_id,
            canonical_fund_id,
            snapshot_date,
            cost_basis_usd,
            fair_value_usd,
            valuation_method,
            ownership_percentage,
            investment_stage,
            sector,
            last_modified_date
        ) as record_hash

    from enhanced_snapshots
)

select * from final