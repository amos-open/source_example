{{
  config(
    materialized='table',
    tags=['intermediate', 'snapshots']
  )
}}

/*
  Intermediate model for fund-level NAV snapshot preparation
  
  This model prepares fund-level NAV calculations and performance metrics,
  combining data from fund administration systems with entity resolution
  to create comprehensive fund performance snapshots.
  
  Data sources:
  - Fund administration NAV data (primary)
  - Fund cross-reference mappings for entity resolution
  - Currency exchange rates for conversion
  
  Business logic:
  - Standardize NAV calculations and performance metrics
  - Apply currency conversion to base currency
  - Calculate derived performance indicators and benchmarks
  - Prepare fund performance data for canonical consumption
*/

with admin_nav_fund as (
    select * from {{ ref('stg_admin_nav_fund') }}
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

-- Prepare fund NAV snapshots with entity resolution
fund_nav_snapshots as (
    select
        anf.nav_id as snapshot_id,
        'FUND_NAV' as snapshot_type,
        'FUND_ADMIN_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || anf.fund_code) as canonical_fund_id,
        anf.fund_code as source_fund_id,
        fx.canonical_fund_name as fund_name,
        
        -- Snapshot date
        anf.valuation_date as snapshot_date,
        anf.valuation_quarter,
        anf.valuation_year,
        
        -- NAV metrics in original currency
        anf.nav_per_share,
        anf.total_nav,
        anf.validated_nav_currency as original_currency,
        
        -- Capital flow metrics in original currency
        anf.committed_capital,
        anf.called_capital,
        anf.distributed_capital,
        anf.remaining_value,
        anf.total_value,
        
        -- Performance ratios (currency independent)
        anf.dpi_ratio,
        anf.rvpi_ratio,
        anf.tvpi_ratio,
        anf.calculated_dpi,
        anf.calculated_rvpi,
        anf.calculated_tvpi,
        
        -- IRR metrics (as decimals)
        anf.irr_gross,
        anf.irr_net,
        
        -- Fee and expense tracking
        anf.management_fees_paid,
        anf.carried_interest_paid,
        anf.fund_expenses,
        
        -- Portfolio metrics
        anf.number_of_investments,
        anf.average_investment_size,
        
        -- Derived metrics
        anf.capital_deployment_percentage,
        anf.capital_returned_percentage,
        anf.management_fee_burden_percentage,
        anf.carry_burden_percentage,
        
        -- Performance categorization
        anf.tvpi_performance_category,
        anf.irr_performance_category,
        anf.overall_performance_quartile,
        
        -- Data quality indicators
        anf.dpi_calculation_variance,
        anf.rvpi_calculation_variance,
        anf.tvpi_calculation_variance,
        anf.data_quality_assessment,
        anf.completeness_score,
        
        -- Audit fields
        anf.created_date,
        anf.last_modified_date,
        current_timestamp() as processed_at

    from admin_nav_fund anf
    left join fund_xref fx on anf.fund_code = fx.admin_fund_code
),

-- Apply currency conversion to USD base currency
currency_converted_snapshots as (
    select
        *,
        
        -- Convert NAV metrics to USD
        case 
            when original_currency = 'USD' then total_nav
            when fx_nav.exchange_rate is not null then total_nav * fx_nav.exchange_rate
            else total_nav  -- Keep original if no rate available
        end as total_nav_usd,
        
        case 
            when original_currency = 'USD' then committed_capital
            when fx_nav.exchange_rate is not null then committed_capital * fx_nav.exchange_rate
            else committed_capital
        end as committed_capital_usd,
        
        case 
            when original_currency = 'USD' then called_capital
            when fx_nav.exchange_rate is not null then called_capital * fx_nav.exchange_rate
            else called_capital
        end as called_capital_usd,
        
        case 
            when original_currency = 'USD' then distributed_capital
            when fx_nav.exchange_rate is not null then distributed_capital * fx_nav.exchange_rate
            else distributed_capital
        end as distributed_capital_usd,
        
        case 
            when original_currency = 'USD' then remaining_value
            when fx_nav.exchange_rate is not null then remaining_value * fx_nav.exchange_rate
            else remaining_value
        end as remaining_value_usd,
        
        case 
            when original_currency = 'USD' then total_value
            when fx_nav.exchange_rate is not null then total_value * fx_nav.exchange_rate
            else total_value
        end as total_value_usd,
        
        -- Convert fee and expense metrics to USD
        case 
            when original_currency = 'USD' then management_fees_paid
            when fx_nav.exchange_rate is not null then management_fees_paid * fx_nav.exchange_rate
            else management_fees_paid
        end as management_fees_paid_usd,
        
        case 
            when original_currency = 'USD' then carried_interest_paid
            when fx_nav.exchange_rate is not null then carried_interest_paid * fx_nav.exchange_rate
            else carried_interest_paid
        end as carried_interest_paid_usd,
        
        case 
            when original_currency = 'USD' then fund_expenses
            when fx_nav.exchange_rate is not null then fund_expenses * fx_nav.exchange_rate
            else fund_expenses
        end as fund_expenses_usd,
        
        case 
            when original_currency = 'USD' then average_investment_size
            when fx_nav.exchange_rate is not null then average_investment_size * fx_nav.exchange_rate
            else average_investment_size
        end as average_investment_size_usd,
        
        -- Currency conversion metadata
        fx_nav.exchange_rate as fx_rate,
        case 
            when original_currency != 'USD' and fx_nav.exchange_rate is not null then true
            else false
        end as currency_converted

    from fund_nav_snapshots fns
    left join current_fx_rates fx_nav 
        on fns.original_currency = fx_nav.from_currency 
        and fx_nav.to_currency = 'USD'
),

-- Add enhanced analytics and benchmarking
enhanced_snapshots as (
    select
        *,
        
        -- Fund size categorization (in USD)
        case 
            when committed_capital_usd >= 5000000000 then 'MEGA_FUND'      -- $5B+
            when committed_capital_usd >= 1000000000 then 'LARGE_FUND'     -- $1B-$5B
            when committed_capital_usd >= 500000000 then 'MID_FUND'        -- $500M-$1B
            when committed_capital_usd >= 100000000 then 'SMALL_FUND'      -- $100M-$500M
            when committed_capital_usd > 0 then 'MICRO_FUND'               -- <$100M
            else 'UNKNOWN'
        end as fund_size_category,
        
        -- Investment pace analysis
        case 
            when capital_deployment_percentage >= 90 then 'FULLY_DEPLOYED'
            when capital_deployment_percentage >= 70 then 'MOSTLY_DEPLOYED'
            when capital_deployment_percentage >= 50 then 'HALF_DEPLOYED'
            when capital_deployment_percentage >= 25 then 'EARLY_DEPLOYMENT'
            when capital_deployment_percentage > 0 then 'INITIAL_DEPLOYMENT'
            else 'NOT_DEPLOYED'
        end as deployment_stage,
        
        -- Distribution activity assessment
        case 
            when capital_returned_percentage >= 100 then 'CAPITAL_RETURNED_PLUS'
            when capital_returned_percentage >= 75 then 'SIGNIFICANT_DISTRIBUTIONS'
            when capital_returned_percentage >= 25 then 'MODERATE_DISTRIBUTIONS'
            when capital_returned_percentage > 0 then 'INITIAL_DISTRIBUTIONS'
            else 'NO_DISTRIBUTIONS'
        end as distribution_stage,
        
        -- Fund lifecycle assessment
        case 
            when deployment_stage in ('FULLY_DEPLOYED', 'MOSTLY_DEPLOYED') 
                and distribution_stage in ('CAPITAL_RETURNED_PLUS', 'SIGNIFICANT_DISTRIBUTIONS') then 'HARVEST_PHASE'
            when deployment_stage in ('FULLY_DEPLOYED', 'MOSTLY_DEPLOYED') 
                and distribution_stage in ('MODERATE_DISTRIBUTIONS', 'INITIAL_DISTRIBUTIONS') then 'VALUE_CREATION_PHASE'
            when deployment_stage in ('HALF_DEPLOYED', 'EARLY_DEPLOYMENT', 'INITIAL_DEPLOYMENT') then 'INVESTMENT_PHASE'
            when deployment_stage = 'NOT_DEPLOYED' then 'FUNDRAISING_PHASE'
            else 'UNKNOWN_PHASE'
        end as fund_lifecycle_phase,
        
        -- Performance benchmarking (industry standard benchmarks)
        case 
            when tvpi_ratio >= 2.5 and irr_net >= 0.20 then 'TOP_DECILE'
            when tvpi_ratio >= 2.0 and irr_net >= 0.15 then 'TOP_QUARTILE'
            when tvpi_ratio >= 1.5 and irr_net >= 0.12 then 'SECOND_QUARTILE'
            when tvpi_ratio >= 1.2 and irr_net >= 0.08 then 'THIRD_QUARTILE'
            when tvpi_ratio >= 1.0 or irr_net >= 0.05 then 'BOTTOM_QUARTILE'
            else 'UNDERPERFORMING'
        end as performance_benchmark,
        
        -- Fee efficiency assessment
        case 
            when management_fee_burden_percentage <= 2.0 then 'LOW_FEE_BURDEN'
            when management_fee_burden_percentage <= 3.0 then 'MODERATE_FEE_BURDEN'
            when management_fee_burden_percentage <= 4.0 then 'HIGH_FEE_BURDEN'
            else 'EXCESSIVE_FEE_BURDEN'
        end as fee_efficiency,
        
        -- Portfolio concentration analysis
        case 
            when number_of_investments >= 50 then 'HIGHLY_DIVERSIFIED'
            when number_of_investments >= 25 then 'WELL_DIVERSIFIED'
            when number_of_investments >= 15 then 'MODERATELY_DIVERSIFIED'
            when number_of_investments >= 8 then 'CONCENTRATED'
            when number_of_investments > 0 then 'HIGHLY_CONCENTRATED'
            else 'NO_INVESTMENTS'
        end as portfolio_diversification,
        
        -- Average investment size relative to fund size
        case 
            when average_investment_size_usd is not null and committed_capital_usd > 0 then
                (average_investment_size_usd / committed_capital_usd) * 100
            else null
        end as average_investment_percentage_of_fund,
        
        -- Investment sizing strategy
        case 
            when average_investment_percentage_of_fund >= 10 then 'LARGE_TICKET_STRATEGY'
            when average_investment_percentage_of_fund >= 5 then 'MEDIUM_TICKET_STRATEGY'
            when average_investment_percentage_of_fund >= 2 then 'SMALL_TICKET_STRATEGY'
            when average_investment_percentage_of_fund > 0 then 'MICRO_TICKET_STRATEGY'
            else 'UNKNOWN_STRATEGY'
        end as investment_sizing_strategy,
        
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
        
        -- Quarter-over-quarter performance indicators (placeholder for future enhancement)
        -- These would require window functions to compare with previous quarters
        
        -- Year-over-year performance indicators (placeholder for future enhancement)
        -- These would require window functions to compare with previous years

    from currency_converted_snapshots
),

-- Final snapshot preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall fund health score (0-100)
        (
            case when performance_benchmark = 'TOP_DECILE' then 25
                 when performance_benchmark = 'TOP_QUARTILE' then 20
                 when performance_benchmark = 'SECOND_QUARTILE' then 15
                 when performance_benchmark = 'THIRD_QUARTILE' then 10
                 when performance_benchmark = 'BOTTOM_QUARTILE' then 5
                 else 0 end +
            case when fund_lifecycle_phase = 'HARVEST_PHASE' then 20
                 when fund_lifecycle_phase = 'VALUE_CREATION_PHASE' then 15
                 when fund_lifecycle_phase = 'INVESTMENT_PHASE' then 10
                 else 5 end +
            case when fee_efficiency = 'LOW_FEE_BURDEN' then 15
                 when fee_efficiency = 'MODERATE_FEE_BURDEN' then 10
                 when fee_efficiency = 'HIGH_FEE_BURDEN' then 5
                 else 0 end +
            case when portfolio_diversification in ('HIGHLY_DIVERSIFIED', 'WELL_DIVERSIFIED') then 15
                 when portfolio_diversification = 'MODERATELY_DIVERSIFIED' then 10
                 when portfolio_diversification = 'CONCENTRATED' then 5
                 else 0 end +
            case when data_quality_assessment = 'HIGH_QUALITY' then 15
                 when data_quality_assessment = 'MEDIUM_QUALITY' then 10
                 else 5 end +
            case when snapshot_freshness in ('CURRENT', 'RECENT') then 10
                 when snapshot_freshness = 'STALE' then 5
                 else 0 end
        ) as fund_health_score,
        
        -- Investment recommendation based on performance and health
        case 
            when fund_health_score >= 85 and performance_benchmark in ('TOP_DECILE', 'TOP_QUARTILE') then 'HIGHLY_RECOMMENDED'
            when fund_health_score >= 70 and performance_benchmark in ('TOP_DECILE', 'TOP_QUARTILE', 'SECOND_QUARTILE') then 'RECOMMENDED'
            when fund_health_score >= 55 and performance_benchmark in ('SECOND_QUARTILE', 'THIRD_QUARTILE') then 'CONSIDER'
            when fund_health_score >= 40 then 'MONITOR'
            else 'AVOID'
        end as investment_recommendation,
        
        -- Data quality flags
        case 
            when currency_converted and fx_rate is null then 'FX_RATE_MISSING'
            when dpi_calculation_variance or rvpi_calculation_variance or tvpi_calculation_variance then 'CALCULATION_VARIANCE'
            when completeness_score < 70 then 'INCOMPLETE_DATA'
            when snapshot_freshness = 'OUTDATED' then 'STALE_DATA'
            when data_quality_assessment = 'CALCULATION_ISSUES' then 'CALCULATION_ISSUES'
            else 'NO_ISSUES'
        end as data_quality_flag,
        
        -- Monitoring priority for fund managers
        case 
            when performance_benchmark in ('UNDERPERFORMING', 'BOTTOM_QUARTILE') then 'HIGH_PRIORITY'
            when fund_lifecycle_phase = 'HARVEST_PHASE' and distribution_stage = 'NO_DISTRIBUTIONS' then 'HIGH_PRIORITY'
            when fee_efficiency = 'EXCESSIVE_FEE_BURDEN' then 'MEDIUM_PRIORITY'
            when snapshot_freshness in ('STALE', 'OUTDATED') then 'MEDIUM_PRIORITY'
            else 'LOW_PRIORITY'
        end as monitoring_priority,
        
        -- Record hash for change detection
        hash(
            snapshot_id,
            canonical_fund_id,
            snapshot_date,
            total_nav_usd,
            committed_capital_usd,
            called_capital_usd,
            distributed_capital_usd,
            tvpi_ratio,
            irr_net,
            number_of_investments,
            last_modified_date
        ) as record_hash

    from enhanced_snapshots
)

select * from final