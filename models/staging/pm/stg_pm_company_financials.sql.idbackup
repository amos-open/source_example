{{
  config(
    materialized='view',
    tags=['portfolio_mgmt', 'staging']
  )
}}

/*
  Staging model for portfolio management company financials data
  
  This model cleans and standardizes company financial data from the portfolio management system,
  handling financial statements, ratios, and operational metrics.
  
  Transformations applied:
  - Validate and standardize financial amounts
  - Parse and validate reporting periods
  - Calculate derived financial metrics and ratios
  - Handle currency standardization
  - Add financial health assessment and categorization
*/

with source as (
    select * from {{ source('portfolio_management_vendor', 'amos_pm_company_financials') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(financial_id) as financial_id,
        trim(company_id) as company_id,
        trim(company_name) as company_name,
        
        -- Reporting period information
        trim(reporting_period) as reporting_period,
        
        case 
            when reporting_year is not null 
                and reporting_year between 2000 and year(current_date()) + 1
            then cast(reporting_year as number(4,0))
            else null
        end as reporting_year,
        
        case 
            when reporting_quarter is not null 
                and reporting_quarter between 1 and 4
            then cast(reporting_quarter as number(1,0))
            else null
        end as reporting_quarter,
        
        -- Income statement items
        case 
            when revenue is not null and revenue >= 0
            then cast(revenue as number(24,2))
            else null
        end as revenue,
        
        upper(trim(revenue_currency)) as revenue_currency,
        
        case 
            when gross_profit is not null 
            then cast(gross_profit as number(24,2))
            else null
        end as gross_profit,
        
        case 
            when gross_margin_percent is not null 
                and gross_margin_percent between -100 and 100
            then cast(gross_margin_percent as number(8,4))
            else null
        end as gross_margin_percent,
        
        case 
            when ebitda is not null 
            then cast(ebitda as number(24,2))
            else null
        end as ebitda,
        
        case 
            when ebitda_margin_percent is not null 
                and ebitda_margin_percent between -100 and 100
            then cast(ebitda_margin_percent as number(8,4))
            else null
        end as ebitda_margin_percent,
        
        case 
            when net_income is not null 
            then cast(net_income as number(24,2))
            else null
        end as net_income,
        
        upper(trim(net_income_currency)) as net_income_currency,
        
        -- Balance sheet items
        case 
            when total_assets is not null and total_assets >= 0
            then cast(total_assets as number(24,2))
            else null
        end as total_assets,
        
        case 
            when total_liabilities is not null and total_liabilities >= 0
            then cast(total_liabilities as number(24,2))
            else null
        end as total_liabilities,
        
        case 
            when shareholders_equity is not null 
            then cast(shareholders_equity as number(24,2))
            else null
        end as shareholders_equity,
        
        case 
            when cash_and_equivalents is not null and cash_and_equivalents >= 0
            then cast(cash_and_equivalents as number(24,2))
            else null
        end as cash_and_equivalents,
        
        case 
            when accounts_receivable is not null and accounts_receivable >= 0
            then cast(accounts_receivable as number(24,2))
            else null
        end as accounts_receivable,
        
        case 
            when inventory is not null and inventory >= 0
            then cast(inventory as number(24,2))
            else null
        end as inventory,
        
        case 
            when accounts_payable is not null and accounts_payable >= 0
            then cast(accounts_payable as number(24,2))
            else null
        end as accounts_payable,
        
        -- Debt information
        case 
            when debt_total is not null and debt_total >= 0
            then cast(debt_total as number(24,2))
            else null
        end as debt_total,
        
        case 
            when debt_current is not null and debt_current >= 0
            then cast(debt_current as number(24,2))
            else null
        end as debt_current,
        
        case 
            when debt_long_term is not null and debt_long_term >= 0
            then cast(debt_long_term as number(24,2))
            else null
        end as debt_long_term,
        
        -- Cash flow items
        case 
            when capex is not null 
            then cast(capex as number(24,2))
            else null
        end as capex,
        
        case 
            when free_cash_flow is not null 
            then cast(free_cash_flow as number(24,2))
            else null
        end as free_cash_flow,
        
        case 
            when working_capital is not null 
            then cast(working_capital as number(24,2))
            else null
        end as working_capital,
        
        -- Operational metrics
        case 
            when employees_count is not null and employees_count >= 0
            then cast(employees_count as number(10,0))
            else null
        end as employees_count,
        
        -- Audit fields
        case 
            when created_date is not null 
            then cast(created_date as date)
            else null
        end as created_date,
        
        case 
            when last_modified_date is not null 
            then cast(last_modified_date as date)
            else null
        end as last_modified_date,
        
        -- Source system metadata
        'PORTFOLIO_MGMT_VENDOR' as source_system,
        'amos_pm_company_financials' as source_table,
        current_timestamp() as loaded_at

    from source
    where financial_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when revenue_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then revenue_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_revenue_currency,
        
        -- Calculate derived financial metrics
        case 
            when revenue is not null and revenue > 0 and gross_profit is not null
            then (gross_profit / revenue) * 100
            else null
        end as calculated_gross_margin_percent,
        
        case 
            when revenue is not null and revenue > 0 and ebitda is not null
            then (ebitda / revenue) * 100
            else null
        end as calculated_ebitda_margin_percent,
        
        case 
            when revenue is not null and revenue > 0 and net_income is not null
            then (net_income / revenue) * 100
            else null
        end as net_margin_percent,
        
        -- Balance sheet ratios
        case 
            when total_assets is not null and total_assets > 0 and total_liabilities is not null
            then (total_liabilities / total_assets) * 100
            else null
        end as debt_to_assets_ratio,
        
        case 
            when shareholders_equity is not null and shareholders_equity > 0 and debt_total is not null
            then debt_total / shareholders_equity
            else null
        end as debt_to_equity_ratio,
        
        case 
            when total_assets is not null and total_assets > 0 and shareholders_equity is not null
            then (shareholders_equity / total_assets) * 100
            else null
        end as equity_ratio_percent,
        
        -- Liquidity ratios
        case 
            when debt_current is not null and debt_current > 0 and cash_and_equivalents is not null
            then cash_and_equivalents / debt_current
            else null
        end as cash_ratio,
        
        -- Efficiency ratios
        case 
            when revenue is not null and revenue > 0 and total_assets is not null and total_assets > 0
            then revenue / total_assets
            else null
        end as asset_turnover_ratio,
        
        case 
            when revenue is not null and revenue > 0 and employees_count is not null and employees_count > 0
            then revenue / employees_count
            else null
        end as revenue_per_employee,
        
        -- Working capital analysis
        case 
            when accounts_receivable is not null and accounts_payable is not null and inventory is not null
            then (accounts_receivable + inventory - accounts_payable)
            else null
        end as calculated_working_capital,
        
        case 
            when revenue is not null and revenue > 0 and working_capital is not null
            then (working_capital / revenue) * 100
            else null
        end as working_capital_as_percent_revenue,
        
        -- Growth metrics (requires historical data - placeholder for future enhancement)
        -- These would be calculated in intermediate models with window functions
        
        -- Financial health indicators
        case 
            when ebitda is not null and ebitda > 0 then 'PROFITABLE_EBITDA'
            when gross_profit is not null and gross_profit > 0 then 'POSITIVE_GROSS_PROFIT'
            when revenue is not null and revenue > 0 then 'REVENUE_GENERATING'
            else 'PRE_REVENUE'
        end as profitability_stage,
        
        case 
            when cash_and_equivalents is not null and debt_current is not null then
                case 
                    when cash_and_equivalents >= debt_current * 2 then 'STRONG_LIQUIDITY'
                    when cash_and_equivalents >= debt_current then 'ADEQUATE_LIQUIDITY'
                    when cash_and_equivalents >= debt_current * 0.5 then 'TIGHT_LIQUIDITY'
                    else 'LIQUIDITY_CONCERN'
                end
            else 'UNKNOWN_LIQUIDITY'
        end as liquidity_assessment,
        
        -- Debt burden assessment
        case 
            when debt_to_equity_ratio is null then 'NO_DEBT_DATA'
            when debt_to_equity_ratio = 0 then 'DEBT_FREE'
            when debt_to_equity_ratio <= 0.5 then 'LOW_LEVERAGE'
            when debt_to_equity_ratio <= 1.0 then 'MODERATE_LEVERAGE'
            when debt_to_equity_ratio <= 2.0 then 'HIGH_LEVERAGE'
            else 'EXCESSIVE_LEVERAGE'
        end as leverage_assessment,
        
        -- Margin quality assessment
        case 
            when gross_margin_percent >= 70 then 'EXCELLENT_MARGINS'
            when gross_margin_percent >= 50 then 'GOOD_MARGINS'
            when gross_margin_percent >= 30 then 'FAIR_MARGINS'
            when gross_margin_percent >= 10 then 'POOR_MARGINS'
            when gross_margin_percent < 10 then 'VERY_POOR_MARGINS'
            else 'UNKNOWN_MARGINS'
        end as margin_quality,
        
        -- Company size categorization
        case 
            when revenue >= 1000000000 then 'LARGE_CAP'      -- $1B+ revenue
            when revenue >= 100000000 then 'MID_CAP'         -- $100M-$1B revenue
            when revenue >= 10000000 then 'SMALL_CAP'        -- $10M-$100M revenue
            when revenue > 0 then 'MICRO_CAP'                -- <$10M revenue
            else 'PRE_REVENUE'
        end as company_size_category,
        
        -- Validation flags for calculated vs reported metrics
        case 
            when abs(coalesce(gross_margin_percent, 0) - coalesce(calculated_gross_margin_percent, 0)) > 1 then true
            else false
        end as gross_margin_calculation_variance,
        
        case 
            when abs(coalesce(ebitda_margin_percent, 0) - coalesce(calculated_ebitda_margin_percent, 0)) > 1 then true
            else false
        end as ebitda_margin_calculation_variance

    from cleaned
),

final as (
    select
        *,
        
        -- Overall financial health score (0-100)
        (
            case 
                when profitability_stage = 'PROFITABLE_EBITDA' then 30
                when profitability_stage = 'POSITIVE_GROSS_PROFIT' then 20
                when profitability_stage = 'REVENUE_GENERATING' then 10
                else 0
            end +
            case 
                when liquidity_assessment = 'STRONG_LIQUIDITY' then 25
                when liquidity_assessment = 'ADEQUATE_LIQUIDITY' then 20
                when liquidity_assessment = 'TIGHT_LIQUIDITY' then 10
                else 0
            end +
            case 
                when leverage_assessment in ('DEBT_FREE', 'LOW_LEVERAGE') then 25
                when leverage_assessment = 'MODERATE_LEVERAGE' then 20
                when leverage_assessment = 'HIGH_LEVERAGE' then 10
                else 0
            end +
            case 
                when margin_quality in ('EXCELLENT_MARGINS', 'GOOD_MARGINS') then 20
                when margin_quality = 'FAIR_MARGINS' then 15
                when margin_quality = 'POOR_MARGINS' then 5
                else 0
            end
        ) as financial_health_score,
        
        -- Data quality assessment
        case 
            when gross_margin_calculation_variance = true 
                or ebitda_margin_calculation_variance = true
            then 'CALCULATION_ISSUES'
            when revenue is not null 
                and gross_profit is not null 
                and ebitda is not null 
                and total_assets is not null
            then 'HIGH_QUALITY'
            when revenue is not null 
                and (gross_profit is not null or ebitda is not null)
            then 'MEDIUM_QUALITY'
            else 'LOW_QUALITY'
        end as data_quality_assessment,
        
        -- Completeness score
        (
            case when revenue is not null then 1 else 0 end +
            case when gross_profit is not null then 1 else 0 end +
            case when ebitda is not null then 1 else 0 end +
            case when net_income is not null then 1 else 0 end +
            case when total_assets is not null then 1 else 0 end +
            case when cash_and_equivalents is not null then 1 else 0 end +
            case when debt_total is not null then 1 else 0 end +
            case when employees_count is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        hash(
            financial_id,
            company_id,
            reporting_year,
            reporting_quarter,
            revenue,
            gross_profit,
            ebitda,
            net_income,
            total_assets,
            cash_and_equivalents,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final