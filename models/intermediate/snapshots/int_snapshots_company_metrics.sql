{{
  config(
    materialized='table',
    tags=['intermediate', 'snapshots']
  )
}}

/*
  Intermediate model for company performance metrics snapshot preparation
  
  This model aggregates company performance data from portfolio management systems,
  combining financial data with operational metrics to create comprehensive
  company performance snapshots for portfolio monitoring and analysis.
  
  Data sources:
  - Portfolio management company financials (primary)
  - Company cross-reference mappings for entity resolution
  - Currency exchange rates for conversion
  
  Business logic:
  - Aggregate financial performance across reporting periods
  - Calculate growth rates and performance trends
  - Apply currency conversion to base currency
  - Generate comprehensive company performance analytics
*/

with pm_company_financials as (
    select * from {{ ref('stg_pm_company_financials') }}
),

company_xref as (
    select * from {{ ref('stg_ref_xref_companies') }}
    where data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
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

-- Get the most recent financial data for each company
latest_financials as (
    select
        *,
        row_number() over (
            partition by company_id 
            order by reporting_year desc, reporting_quarter desc
        ) as rn
    from pm_company_financials
    where reporting_year is not null
),

current_financials as (
    select * from latest_financials where rn = 1
),

-- Calculate year-over-year growth metrics
yoy_growth_metrics as (
    select
        current_year.company_id,
        current_year.company_name,
        current_year.reporting_year as current_year,
        current_year.revenue as current_revenue,
        current_year.ebitda as current_ebitda,
        current_year.net_income as current_net_income,
        current_year.total_assets as current_total_assets,
        current_year.employees_count as current_employees,
        
        prior_year.reporting_year as prior_year,
        prior_year.revenue as prior_revenue,
        prior_year.ebitda as prior_ebitda,
        prior_year.net_income as prior_net_income,
        prior_year.total_assets as prior_total_assets,
        prior_year.employees_count as prior_employees,
        
        -- Calculate growth rates
        case 
            when prior_year.revenue is not null and prior_year.revenue > 0 and current_year.revenue is not null
            then ((current_year.revenue - prior_year.revenue) / prior_year.revenue) * 100
            else null
        end as revenue_growth_yoy_percent,
        
        case 
            when prior_year.ebitda is not null and prior_year.ebitda > 0 and current_year.ebitda is not null
            then ((current_year.ebitda - prior_year.ebitda) / prior_year.ebitda) * 100
            else null
        end as ebitda_growth_yoy_percent,
        
        case 
            when prior_year.total_assets is not null and prior_year.total_assets > 0 and current_year.total_assets is not null
            then ((current_year.total_assets - prior_year.total_assets) / prior_year.total_assets) * 100
            else null
        end as assets_growth_yoy_percent,
        
        case 
            when prior_year.employees_count is not null and prior_year.employees_count > 0 and current_year.employees_count is not null
            then ((current_year.employees_count - prior_year.employees_count) / prior_year.employees_count::float) * 100
            else null
        end as employee_growth_yoy_percent

    from current_financials current_year
    left join pm_company_financials prior_year 
        on current_year.company_id = prior_year.company_id
        and prior_year.reporting_year = current_year.reporting_year - 1
        and prior_year.reporting_quarter = current_year.reporting_quarter
),

-- Prepare company metrics snapshots with entity resolution
company_metrics_snapshots as (
    select
        cf.financial_id || '_METRICS' as snapshot_id,
        'COMPANY_METRICS' as snapshot_type,
        'PORTFOLIO_MGMT_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || cf.company_id) as canonical_company_id,
        cf.company_id as source_company_id,
        cf.company_name,
        cx.canonical_company_name,
        
        -- Snapshot period information
        cf.reporting_year || '-Q' || cf.reporting_quarter as reporting_period,
        cf.reporting_year,
        cf.reporting_quarter,
        
        -- Create snapshot date (last day of quarter)
        case 
            when cf.reporting_quarter = 1 then date(cf.reporting_year || '-03-31')
            when cf.reporting_quarter = 2 then date(cf.reporting_year || '-06-30')
            when cf.reporting_quarter = 3 then date(cf.reporting_year || '-09-30')
            when cf.reporting_quarter = 4 then date(cf.reporting_year || '-12-31')
            else null
        end as snapshot_date,
        
        -- Financial metrics in original currency
        cf.revenue,
        cf.validated_revenue_currency as revenue_currency,
        cf.gross_profit,
        cf.ebitda,
        cf.net_income,
        cf.total_assets,
        cf.total_liabilities,
        cf.shareholders_equity,
        cf.cash_and_equivalents,
        cf.debt_total,
        cf.free_cash_flow,
        cf.working_capital,
        cf.capex,
        
        -- Financial ratios and margins
        cf.calculated_gross_margin_percent as gross_margin_percent,
        cf.calculated_ebitda_margin_percent as ebitda_margin_percent,
        cf.net_margin_percent,
        cf.debt_to_assets_ratio,
        cf.debt_to_equity_ratio,
        cf.equity_ratio_percent,
        cf.cash_ratio,
        cf.asset_turnover_ratio,
        
        -- Operational metrics
        cf.employees_count,
        cf.revenue_per_employee,
        
        -- Growth metrics from YoY analysis
        yoy.revenue_growth_yoy_percent,
        yoy.ebitda_growth_yoy_percent,
        yoy.assets_growth_yoy_percent,
        yoy.employee_growth_yoy_percent,
        
        -- Performance categorizations
        cf.profitability_stage,
        cf.liquidity_assessment,
        cf.leverage_assessment,
        cf.margin_quality,
        cf.company_size_category,
        cf.financial_health_score,
        
        -- Data quality indicators
        cf.gross_margin_calculation_variance,
        cf.ebitda_margin_calculation_variance,
        cf.data_quality_assessment,
        cf.completeness_score,
        
        -- Audit fields
        cf.created_date,
        cf.last_modified_date,
        current_timestamp() as processed_at

    from current_financials cf
    left join company_xref cx on cf.company_id = cx.pm_company_id
    left join yoy_growth_metrics yoy on cf.company_id = yoy.company_id
),

-- Apply currency conversion to USD base currency
currency_converted_snapshots as (
    select
        *,
        
        -- Convert financial metrics to USD
        case 
            when revenue_currency = 'USD' then revenue
            when fx_revenue.exchange_rate is not null then revenue * fx_revenue.exchange_rate
            else revenue  -- Keep original if no rate available
        end as revenue_usd,
        
        case 
            when revenue_currency = 'USD' then gross_profit
            when fx_revenue.exchange_rate is not null then gross_profit * fx_revenue.exchange_rate
            else gross_profit
        end as gross_profit_usd,
        
        case 
            when revenue_currency = 'USD' then ebitda
            when fx_revenue.exchange_rate is not null then ebitda * fx_revenue.exchange_rate
            else ebitda
        end as ebitda_usd,
        
        case 
            when revenue_currency = 'USD' then net_income
            when fx_revenue.exchange_rate is not null then net_income * fx_revenue.exchange_rate
            else net_income
        end as net_income_usd,
        
        case 
            when revenue_currency = 'USD' then total_assets
            when fx_revenue.exchange_rate is not null then total_assets * fx_revenue.exchange_rate
            else total_assets
        end as total_assets_usd,
        
        case 
            when revenue_currency = 'USD' then cash_and_equivalents
            when fx_revenue.exchange_rate is not null then cash_and_equivalents * fx_revenue.exchange_rate
            else cash_and_equivalents
        end as cash_and_equivalents_usd,
        
        case 
            when revenue_currency = 'USD' then debt_total
            when fx_revenue.exchange_rate is not null then debt_total * fx_revenue.exchange_rate
            else debt_total
        end as debt_total_usd,
        
        case 
            when revenue_currency = 'USD' then free_cash_flow
            when fx_revenue.exchange_rate is not null then free_cash_flow * fx_revenue.exchange_rate
            else free_cash_flow
        end as free_cash_flow_usd,
        
        case 
            when revenue_currency = 'USD' then working_capital
            when fx_revenue.exchange_rate is not null then working_capital * fx_revenue.exchange_rate
            else working_capital
        end as working_capital_usd,
        
        case 
            when revenue_currency = 'USD' then capex
            when fx_revenue.exchange_rate is not null then capex * fx_revenue.exchange_rate
            else capex
        end as capex_usd,
        
        -- Currency conversion metadata
        fx_revenue.exchange_rate as fx_rate,
        case 
            when revenue_currency != 'USD' and fx_revenue.exchange_rate is not null then true
            else false
        end as currency_converted

    from company_metrics_snapshots cms
    left join current_fx_rates fx_revenue 
        on cms.revenue_currency = fx_revenue.from_currency 
        and fx_revenue.to_currency = 'USD'
),

-- Add enhanced analytics and performance assessment
enhanced_snapshots as (
    select
        *,
        
        -- Revenue scale categorization (in USD)
        case 
            when revenue_usd >= 1000000000 then 'BILLION_PLUS_REVENUE'     -- $1B+
            when revenue_usd >= 500000000 then 'LARGE_REVENUE'             -- $500M-$1B
            when revenue_usd >= 100000000 then 'MEDIUM_LARGE_REVENUE'      -- $100M-$500M
            when revenue_usd >= 25000000 then 'MEDIUM_REVENUE'             -- $25M-$100M
            when revenue_usd >= 5000000 then 'SMALL_REVENUE'               -- $5M-$25M
            when revenue_usd > 0 then 'MICRO_REVENUE'                      -- <$5M
            else 'PRE_REVENUE'
        end as revenue_scale_category,
        
        -- Growth trajectory assessment
        case 
            when revenue_growth_yoy_percent >= 100 then 'HYPER_GROWTH'
            when revenue_growth_yoy_percent >= 50 then 'HIGH_GROWTH'
            when revenue_growth_yoy_percent >= 25 then 'STRONG_GROWTH'
            when revenue_growth_yoy_percent >= 10 then 'MODERATE_GROWTH'
            when revenue_growth_yoy_percent >= 0 then 'SLOW_GROWTH'
            when revenue_growth_yoy_percent < 0 then 'DECLINING'
            else 'UNKNOWN_GROWTH'
        end as growth_trajectory,
        
        -- Profitability progression assessment
        case 
            when ebitda_usd > 0 and net_income_usd > 0 then 'FULLY_PROFITABLE'
            when ebitda_usd > 0 and net_income_usd <= 0 then 'EBITDA_POSITIVE'
            when gross_profit_usd > 0 and ebitda_usd <= 0 then 'GROSS_PROFIT_POSITIVE'
            when revenue_usd > 0 and gross_profit_usd <= 0 then 'REVENUE_GENERATING'
            else 'PRE_REVENUE'
        end as profitability_progression,
        
        -- Efficiency metrics
        case 
            when revenue_per_employee >= 500000 then 'HIGH_EFFICIENCY'
            when revenue_per_employee >= 250000 then 'GOOD_EFFICIENCY'
            when revenue_per_employee >= 100000 then 'AVERAGE_EFFICIENCY'
            when revenue_per_employee > 0 then 'LOW_EFFICIENCY'
            else 'UNKNOWN_EFFICIENCY'
        end as operational_efficiency,
        
        -- Capital efficiency assessment
        case 
            when asset_turnover_ratio >= 2.0 then 'HIGH_CAPITAL_EFFICIENCY'
            when asset_turnover_ratio >= 1.0 then 'GOOD_CAPITAL_EFFICIENCY'
            when asset_turnover_ratio >= 0.5 then 'AVERAGE_CAPITAL_EFFICIENCY'
            when asset_turnover_ratio > 0 then 'LOW_CAPITAL_EFFICIENCY'
            else 'UNKNOWN_CAPITAL_EFFICIENCY'
        end as capital_efficiency,
        
        -- Cash generation assessment
        case 
            when free_cash_flow_usd > 0 and free_cash_flow_usd >= revenue_usd * 0.15 then 'STRONG_CASH_GENERATION'
            when free_cash_flow_usd > 0 and free_cash_flow_usd >= revenue_usd * 0.05 then 'GOOD_CASH_GENERATION'
            when free_cash_flow_usd > 0 then 'POSITIVE_CASH_GENERATION'
            when free_cash_flow_usd <= 0 then 'CASH_BURN'
            else 'UNKNOWN_CASH_GENERATION'
        end as cash_generation_quality,
        
        -- Investment intensity assessment
        case 
            when capex_usd is not null and revenue_usd > 0 then
                case 
                    when abs(capex_usd) / revenue_usd >= 0.15 then 'HIGH_CAPEX_INTENSITY'
                    when abs(capex_usd) / revenue_usd >= 0.08 then 'MEDIUM_CAPEX_INTENSITY'
                    when abs(capex_usd) / revenue_usd >= 0.03 then 'LOW_CAPEX_INTENSITY'
                    else 'MINIMAL_CAPEX'
                end
            else 'UNKNOWN_CAPEX_INTENSITY'
        end as capex_intensity,
        
        -- Working capital efficiency
        case 
            when working_capital_usd is not null and revenue_usd > 0 then
                case 
                    when working_capital_usd / revenue_usd <= 0.05 then 'EFFICIENT_WORKING_CAPITAL'
                    when working_capital_usd / revenue_usd <= 0.15 then 'AVERAGE_WORKING_CAPITAL'
                    when working_capital_usd / revenue_usd <= 0.25 then 'HIGH_WORKING_CAPITAL'
                    else 'EXCESSIVE_WORKING_CAPITAL'
                end
            else 'UNKNOWN_WORKING_CAPITAL_EFFICIENCY'
        end as working_capital_efficiency,
        
        -- Overall business model assessment
        case 
            when profitability_progression = 'FULLY_PROFITABLE' 
                and growth_trajectory in ('HYPER_GROWTH', 'HIGH_GROWTH', 'STRONG_GROWTH')
                and cash_generation_quality in ('STRONG_CASH_GENERATION', 'GOOD_CASH_GENERATION') then 'EXCEPTIONAL_BUSINESS_MODEL'
            when profitability_progression in ('FULLY_PROFITABLE', 'EBITDA_POSITIVE') 
                and growth_trajectory in ('STRONG_GROWTH', 'MODERATE_GROWTH')
                and cash_generation_quality != 'CASH_BURN' then 'STRONG_BUSINESS_MODEL'
            when profitability_progression in ('EBITDA_POSITIVE', 'GROSS_PROFIT_POSITIVE') 
                and growth_trajectory in ('MODERATE_GROWTH', 'SLOW_GROWTH') then 'DEVELOPING_BUSINESS_MODEL'
            when growth_trajectory = 'DECLINING' 
                or profitability_progression = 'PRE_REVENUE' then 'CHALLENGED_BUSINESS_MODEL'
            else 'UNKNOWN_BUSINESS_MODEL'
        end as business_model_assessment,
        
        -- Snapshot recency assessment
        case 
            when snapshot_date is not null then
                datediff('day', snapshot_date, current_date())
            else null
        end as days_since_snapshot,
        
        case 
            when days_since_snapshot <= 90 then 'CURRENT'
            when days_since_snapshot <= 180 then 'RECENT'
            when days_since_snapshot <= 365 then 'STALE'
            else 'OUTDATED'
        end as snapshot_freshness

    from currency_converted_snapshots
),

-- Final snapshot preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall company performance score (0-100)
        (
            case when profitability_progression = 'FULLY_PROFITABLE' then 25
                 when profitability_progression = 'EBITDA_POSITIVE' then 20
                 when profitability_progression = 'GROSS_PROFIT_POSITIVE' then 15
                 when profitability_progression = 'REVENUE_GENERATING' then 10
                 else 0 end +
            case when growth_trajectory = 'HYPER_GROWTH' then 25
                 when growth_trajectory = 'HIGH_GROWTH' then 20
                 when growth_trajectory = 'STRONG_GROWTH' then 15
                 when growth_trajectory = 'MODERATE_GROWTH' then 10
                 when growth_trajectory = 'SLOW_GROWTH' then 5
                 else 0 end +
            case when cash_generation_quality = 'STRONG_CASH_GENERATION' then 20
                 when cash_generation_quality = 'GOOD_CASH_GENERATION' then 15
                 when cash_generation_quality = 'POSITIVE_CASH_GENERATION' then 10
                 when cash_generation_quality = 'CASH_BURN' then 0
                 else 5 end +
            case when operational_efficiency = 'HIGH_EFFICIENCY' then 15
                 when operational_efficiency = 'GOOD_EFFICIENCY' then 12
                 when operational_efficiency = 'AVERAGE_EFFICIENCY' then 8
                 when operational_efficiency = 'LOW_EFFICIENCY' then 3
                 else 0 end +
            case when financial_health_score >= 80 then 10
                 when financial_health_score >= 60 then 8
                 when financial_health_score >= 40 then 5
                 when financial_health_score >= 20 then 2
                 else 0 end +
            case when snapshot_freshness in ('CURRENT', 'RECENT') then 5
                 when snapshot_freshness = 'STALE' then 3
                 else 0 end
        ) as company_performance_score,
        
        -- Investment attractiveness assessment
        case 
            when company_performance_score >= 85 and business_model_assessment = 'EXCEPTIONAL_BUSINESS_MODEL' then 'HIGHLY_ATTRACTIVE'
            when company_performance_score >= 70 and business_model_assessment in ('EXCEPTIONAL_BUSINESS_MODEL', 'STRONG_BUSINESS_MODEL') then 'ATTRACTIVE'
            when company_performance_score >= 55 and business_model_assessment in ('STRONG_BUSINESS_MODEL', 'DEVELOPING_BUSINESS_MODEL') then 'MODERATELY_ATTRACTIVE'
            when company_performance_score >= 40 then 'MONITOR_PERFORMANCE'
            else 'UNDERPERFORMING'
        end as investment_attractiveness,
        
        -- Portfolio monitoring priority
        case 
            when growth_trajectory = 'DECLINING' or business_model_assessment = 'CHALLENGED_BUSINESS_MODEL' then 'HIGH_PRIORITY'
            when cash_generation_quality = 'CASH_BURN' and profitability_progression = 'PRE_REVENUE' then 'HIGH_PRIORITY'
            when revenue_scale_category in ('BILLION_PLUS_REVENUE', 'LARGE_REVENUE') and company_performance_score < 60 then 'HIGH_PRIORITY'
            when business_model_assessment = 'EXCEPTIONAL_BUSINESS_MODEL' then 'MEDIUM_PRIORITY'
            when snapshot_freshness in ('STALE', 'OUTDATED') then 'MEDIUM_PRIORITY'
            else 'LOW_PRIORITY'
        end as monitoring_priority,
        
        -- Data quality flags
        case 
            when currency_converted and fx_rate is null then 'FX_RATE_MISSING'
            when gross_margin_calculation_variance or ebitda_margin_calculation_variance then 'CALCULATION_VARIANCE'
            when completeness_score < 70 then 'INCOMPLETE_DATA'
            when snapshot_freshness = 'OUTDATED' then 'STALE_DATA'
            when data_quality_assessment = 'CALCULATION_ISSUES' then 'CALCULATION_ISSUES'
            else 'NO_ISSUES'
        end as data_quality_flag,
        
        -- Value creation opportunity assessment
        case 
            when operational_efficiency = 'LOW_EFFICIENCY' and revenue_scale_category in ('MEDIUM_REVENUE', 'LARGE_REVENUE') then 'OPERATIONAL_IMPROVEMENT'
            when working_capital_efficiency in ('HIGH_WORKING_CAPITAL', 'EXCESSIVE_WORKING_CAPITAL') then 'WORKING_CAPITAL_OPTIMIZATION'
            when leverage_assessment = 'LOW_LEVERAGE' and cash_generation_quality = 'STRONG_CASH_GENERATION' then 'LEVERAGE_OPPORTUNITY'
            when growth_trajectory in ('SLOW_GROWTH', 'MODERATE_GROWTH') and margin_quality in ('EXCELLENT_MARGINS', 'GOOD_MARGINS') then 'GROWTH_ACCELERATION'
            when profitability_progression = 'GROSS_PROFIT_POSITIVE' and revenue_growth_yoy_percent >= 25 then 'PROFITABILITY_IMPROVEMENT'
            else 'MAINTAIN_PERFORMANCE'
        end as value_creation_opportunity,
        
        -- Record hash for change detection
        hash(
            snapshot_id,
            canonical_company_id,
            snapshot_date,
            revenue_usd,
            ebitda_usd,
            net_income_usd,
            total_assets_usd,
            employees_count,
            revenue_growth_yoy_percent,
            ebitda_growth_yoy_percent,
            last_modified_date
        ) as record_hash

    from enhanced_snapshots
)

select * from final