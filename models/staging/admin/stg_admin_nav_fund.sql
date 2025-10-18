{{
  config(
    materialized='view',
    tags=['fund_admin', 'staging']
  )
}}

/*
  Staging model for fund administration fund-level NAV data
  
  This model cleans and standardizes fund-level NAV calculations from the fund administration system,
  handling performance metrics, financial ratios, and temporal data.
  
  Transformations applied:
  - Validate and standardize financial amounts and ratios
  - Parse and validate valuation dates
  - Calculate derived performance metrics
  - Handle currency standardization
  - Add performance categorization and benchmarking
*/

with source as (
    select * from {{ source('fund_admin_vendor', 'amos_admin_nav_fund') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(nav_id) as nav_id,
        trim(fund_code) as fund_code,
        
        -- Valuation date
        case 
            when valuation_date is not null 
            then CAST(valuation_date AS DATE)
            else null
        end as valuation_date,
        
        -- NAV metrics
        case 
            when nav_per_share is not null and nav_per_share > 0
            then CAST(nav_per_share AS NUMERIC(18,8))
            else null
        end as nav_per_share,
        
        case 
            when total_nav is not null and total_nav >= 0
            then CAST(total_nav AS NUMERIC(24,2))
            else null
        end as total_nav,
        
        upper(trim(total_nav_currency)) as total_nav_currency,
        
        -- Capital flow metrics
        case 
            when committed_capital is not null and committed_capital >= 0
            then CAST(committed_capital AS NUMERIC(24,2))
            else null
        end as committed_capital,
        
        case 
            when called_capital is not null and called_capital >= 0
            then CAST(called_capital AS NUMERIC(24,2))
            else null
        end as called_capital,
        
        case 
            when distributed_capital is not null and distributed_capital >= 0
            then CAST(distributed_capital AS NUMERIC(24,2))
            else null
        end as distributed_capital,
        
        case 
            when remaining_value is not null and remaining_value >= 0
            then CAST(remaining_value AS NUMERIC(24,2))
            else null
        end as remaining_value,
        
        case 
            when total_value is not null and total_value >= 0
            then CAST(total_value AS NUMERIC(24,2))
            else null
        end as total_value,
        
        -- Performance ratios
        case 
            when dpi_ratio is not null and dpi_ratio >= 0
            then CAST(dpi_ratio AS NUMERIC(8,4))
            else null
        end as dpi_ratio,
        
        case 
            when rvpi_ratio is not null and rvpi_ratio >= 0
            then CAST(rvpi_ratio AS NUMERIC(8,4))
            else null
        end as rvpi_ratio,
        
        case 
            when tvpi_ratio is not null and tvpi_ratio >= 0
            then CAST(tvpi_ratio AS NUMERIC(8,4))
            else null
        end as tvpi_ratio,
        
        -- IRR metrics (stored as decimals, e.g., 0.25 = 25%)
        case 
            when irr_gross is not null 
            then CAST(irr_gross AS NUMERIC(8,6))
            else null
        end as irr_gross,
        
        case 
            when irr_net is not null 
            then CAST(irr_net AS NUMERIC(8,6))
            else null
        end as irr_net,
        
        -- Fee and expense tracking
        case 
            when management_fees_paid is not null and management_fees_paid >= 0
            then CAST(management_fees_paid AS NUMERIC(20,2))
            else null
        end as management_fees_paid,
        
        case 
            when carried_interest_paid is not null and carried_interest_paid >= 0
            then CAST(carried_interest_paid AS NUMERIC(20,2))
            else null
        end as carried_interest_paid,
        
        case 
            when fund_expenses is not null and fund_expenses >= 0
            then CAST(fund_expenses AS NUMERIC(20,2))
            else null
        end as fund_expenses,
        
        -- Portfolio metrics
        case 
            when number_of_investments is not null and number_of_investments >= 0
            then CAST(number_of_investments AS NUMERIC(5,0))
            else null
        end as number_of_investments,
        
        -- Audit fields
        case 
            when created_date is not null 
            then CAST(created_date AS DATE)
            else null
        end as created_date,
        
        case 
            when last_modified_date is not null 
            then CAST(last_modified_date AS DATE)
            else null
        end as last_modified_date,
        
        -- Source system metadata
        'FUND_ADMIN_VENDOR' as source_system,
        'amos_admin_nav_fund' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where nav_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when total_nav_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then total_nav_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_nav_currency,
        
        -- Calculate derived metrics and validate existing ones
        case 
            when called_capital is not null and called_capital > 0 and distributed_capital is not null
            then distributed_capital / called_capital
            else null
        end as calculated_dpi,
        
        case 
            when called_capital is not null and called_capital > 0 and remaining_value is not null
            then remaining_value / called_capital
            else null
        end as calculated_rvpi,
        
        case 
            when called_capital is not null and called_capital > 0 and total_value is not null
            then total_value / called_capital
            else null
        end as calculated_tvpi,
        
        -- Capital deployment metrics
        case 
            when committed_capital is not null and committed_capital > 0 and called_capital is not null
            then (called_capital / committed_capital) * 100
            else null
        end as capital_deployment_percentage,
        
        case 
            when called_capital is not null and called_capital > 0 and distributed_capital is not null
            then (distributed_capital / called_capital) * 100
            else null
        end as capital_returned_percentage,
        
        -- Fee burden analysis
        case 
            when called_capital is not null and called_capital > 0 and management_fees_paid is not null
            then (management_fees_paid / called_capital) * 100
            else null
        end as management_fee_burden_percentage,
        
        case 
            when total_value is not null and total_value > 0 and carried_interest_paid is not null
            then (carried_interest_paid / total_value) * 100
            else null
        end as carry_burden_percentage,
        
        -- Portfolio concentration
        case 
            when number_of_investments is not null and number_of_investments > 0 and total_nav is not null
            then total_nav / number_of_investments
            else null
        end as average_investment_size,
        
        -- Time-based metrics
        quarter(valuation_date) as valuation_quarter,
        year(valuation_date) as valuation_year,
        
        -- Performance categorization
        case 
            when tvpi_ratio >= 3.0 then 'EXCELLENT'
            when tvpi_ratio >= 2.0 then 'GOOD'
            when tvpi_ratio >= 1.5 then 'FAIR'
            when tvpi_ratio >= 1.0 then 'POOR'
            when tvpi_ratio < 1.0 then 'LOSS'
            else 'UNKNOWN'
        end as tvpi_performance_category,
        
        case 
            when irr_net >= 0.20 then 'EXCELLENT'  -- 20%+ IRR
            when irr_net >= 0.15 then 'GOOD'       -- 15-20% IRR
            when irr_net >= 0.10 then 'FAIR'       -- 10-15% IRR
            when irr_net >= 0.05 then 'POOR'       -- 5-10% IRR
            when irr_net < 0.05 then 'LOSS'        -- <5% IRR
            else 'UNKNOWN'
        end as irr_performance_category,
        
        -- Validation flags for calculated vs reported metrics
        case 
            when abs(coalesce(dpi_ratio, 0) - coalesce(calculated_dpi, 0)) > 0.01 then true
            else false
        end as dpi_calculation_variance,
        
        case 
            when abs(coalesce(rvpi_ratio, 0) - coalesce(calculated_rvpi, 0)) > 0.01 then true
            else false
        end as rvpi_calculation_variance,
        
        case 
            when abs(coalesce(tvpi_ratio, 0) - coalesce(calculated_tvpi, 0)) > 0.01 then true
            else false
        end as tvpi_calculation_variance

    from cleaned
),

final as (
    select
        *,
        
        -- Overall fund performance assessment
        case 
            when tvpi_performance_category in ('EXCELLENT', 'GOOD') 
                and irr_performance_category in ('EXCELLENT', 'GOOD')
            then 'TOP_QUARTILE'
            when tvpi_performance_category in ('EXCELLENT', 'GOOD', 'FAIR') 
                and irr_performance_category in ('EXCELLENT', 'GOOD', 'FAIR')
            then 'ABOVE_MEDIAN'
            when tvpi_performance_category in ('FAIR', 'POOR') 
                and irr_performance_category in ('FAIR', 'POOR')
            then 'BELOW_MEDIAN'
            else 'BOTTOM_QUARTILE'
        end as overall_performance_quartile,
        
        -- Data quality assessment
        case 
            when dpi_calculation_variance = true 
                or rvpi_calculation_variance = true 
                or tvpi_calculation_variance = true
            then 'CALCULATION_ISSUES'
            when total_nav is not null 
                and committed_capital is not null 
                and called_capital is not null 
                and tvpi_ratio is not null 
                and irr_net is not null
            then 'HIGH_QUALITY'
            when total_nav is not null 
                and called_capital is not null 
                and tvpi_ratio is not null
            then 'MEDIUM_QUALITY'
            else 'LOW_QUALITY'
        end as data_quality_assessment,
        
        -- Completeness score
        (
            case when fund_code is not null then 1 else 0 end +
            case when valuation_date is not null then 1 else 0 end +
            case when total_nav is not null then 1 else 0 end +
            case when committed_capital is not null then 1 else 0 end +
            case when called_capital is not null then 1 else 0 end +
            case when tvpi_ratio is not null then 1 else 0 end +
            case when irr_net is not null then 1 else 0 end +
            case when number_of_investments is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(nav_id, fund_code, valuation_date, total_nav, called_capital, distributed_capital, remaining_value, tvpi_ratio, irr_net, last_modified_date)) as record_hash

    from enhanced
)

select * from final