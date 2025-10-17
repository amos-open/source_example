{{
  config(
    materialized='view',
    tags=['fund_admin', 'staging']
  )
}}

/*
  Staging model for fund administration investment-level NAV data
  
  This model cleans and standardizes investment-level NAV calculations from the fund administration system,
  handling valuation methods, performance metrics, and investment characteristics.
  
  Transformations applied:
  - Validate and standardize financial amounts and valuations
  - Parse and validate dates
  - Standardize valuation methods and investment stages
  - Calculate derived performance metrics
  - Handle ownership and governance data
  - Add investment categorization and performance analysis
*/

with source as (
    select * from {{ source('fund_admin_vendor', 'amos_admin_nav_investment') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(nav_investment_id) as nav_investment_id,
        trim(fund_code) as fund_code,
        trim(investment_id) as investment_id,
        
        -- Investment details
        trim(investment_name) as investment_name,
        
        -- Valuation date
        case 
            when valuation_date is not null 
            then cast(valuation_date as date)
            else null
        end as valuation_date,
        
        -- Financial metrics
        case 
            when cost_basis is not null and cost_basis > 0
            then cast(cost_basis as number(20,2))
            else null
        end as cost_basis,
        
        upper(trim(cost_basis_currency)) as cost_basis_currency,
        
        case 
            when fair_value is not null and fair_value >= 0
            then cast(fair_value as number(20,2))
            else null
        end as fair_value,
        
        upper(trim(fair_value_currency)) as fair_value_currency,
        
        case 
            when unrealized_gain_loss is not null 
            then cast(unrealized_gain_loss as number(20,2))
            else null
        end as unrealized_gain_loss,
        
        -- Valuation methodology
        trim(valuation_method) as valuation_method,
        
        case 
            when valuation_multiple is not null and valuation_multiple > 0
            then cast(valuation_multiple as number(8,4))
            else null
        end as valuation_multiple,
        
        case 
            when last_financing_valuation is not null and last_financing_valuation > 0
            then cast(last_financing_valuation as number(20,2))
            else null
        end as last_financing_valuation,
        
        -- Investment characteristics
        case 
            when investment_date is not null 
            then cast(investment_date as date)
            else null
        end as investment_date,
        
        trim(investment_stage) as investment_stage,
        trim(sector) as sector,
        trim(geography) as geography,
        
        -- Ownership and governance
        case 
            when ownership_percentage is not null 
                and ownership_percentage between 0 and 100
            then cast(ownership_percentage as number(8,4))
            else null
        end as ownership_percentage,
        
        case 
            when board_seats is not null and board_seats >= 0
            then cast(board_seats as number(3,0))
            else null
        end as board_seats,
        
        case 
            when liquidation_preference is not null and liquidation_preference > 0
            then cast(liquidation_preference as number(8,4))
            else null
        end as liquidation_preference,
        
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
        'FUND_ADMIN_VENDOR' as source_system,
        'amos_admin_nav_investment' as source_table,
        current_timestamp() as loaded_at

    from source
    where nav_investment_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when cost_basis_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then cost_basis_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_cost_currency,
        
        case 
            when fair_value_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then fair_value_currency
            else cost_basis_currency  -- Default to cost basis currency
        end as validated_fair_value_currency,
        
        -- Calculate derived performance metrics
        case 
            when cost_basis is not null and cost_basis > 0 and fair_value is not null
            then (fair_value / cost_basis) - 1
            else null
        end as unrealized_return_multiple,
        
        case 
            when cost_basis is not null and cost_basis > 0 and unrealized_gain_loss is not null
            then (unrealized_gain_loss / cost_basis) * 100
            else null
        end as unrealized_return_percentage,
        
        -- Validate unrealized gain/loss calculation
        case 
            when cost_basis is not null and fair_value is not null
            then fair_value - cost_basis
            else null
        end as calculated_unrealized_gain_loss,
        
        -- Investment age calculation
        case 
            when investment_date is not null and valuation_date is not null
            then datediff('month', investment_date, valuation_date)
            else null
        end as investment_age_months,
        
        case 
            when investment_date is not null and valuation_date is not null
            then datediff('year', investment_date, valuation_date)
            else null
        end as investment_age_years,
        
        -- Valuation method standardization
        case 
            when upper(valuation_method) like '%MARKET%' 
                or upper(valuation_method) like '%MULTIPLE%' then 'MARKET_MULTIPLE'
            when upper(valuation_method) like '%DCF%' 
                or upper(valuation_method) like '%DISCOUNTED%' then 'DCF'
            when upper(valuation_method) like '%TRANSACTION%' 
                or upper(valuation_method) like '%RECENT%' then 'RECENT_TRANSACTION'
            when upper(valuation_method) like '%COST%' 
                or upper(valuation_method) like '%BOOK%' then 'COST_BASIS'
            when upper(valuation_method) like '%LIQUIDATION%' then 'LIQUIDATION_VALUE'
            else 'OTHER'
        end as standardized_valuation_method,
        
        -- Investment stage standardization
        case 
            when upper(investment_stage) like '%SEED%' then 'SEED'
            when upper(investment_stage) like '%SERIES A%' or upper(investment_stage) like '%EARLY%' then 'EARLY_STAGE'
            when upper(investment_stage) like '%SERIES B%' or upper(investment_stage) like '%GROWTH%' then 'GROWTH'
            when upper(investment_stage) like '%SERIES C%' or upper(investment_stage) like '%LATE%' then 'LATE_STAGE'
            when upper(investment_stage) like '%BUYOUT%' or upper(investment_stage) like '%LBO%' then 'BUYOUT'
            when upper(investment_stage) like '%MEZZANINE%' then 'MEZZANINE'
            when upper(investment_stage) like '%DISTRESSED%' then 'DISTRESSED'
            else 'OTHER'
        end as standardized_investment_stage,
        
        -- Sector standardization
        case 
            when upper(sector) like '%TECHNOLOGY%' or upper(sector) like '%SOFTWARE%' then 'TECHNOLOGY'
            when upper(sector) like '%HEALTHCARE%' or upper(sector) like '%MEDICAL%' then 'HEALTHCARE'
            when upper(sector) like '%FINANCIAL%' or upper(sector) like '%FINTECH%' then 'FINANCIAL_SERVICES'
            when upper(sector) like '%CONSUMER%' or upper(sector) like '%RETAIL%' then 'CONSUMER'
            when upper(sector) like '%INDUSTRIAL%' or upper(sector) like '%MANUFACTURING%' then 'INDUSTRIALS'
            when upper(sector) like '%ENERGY%' or upper(sector) like '%RENEWABLE%' then 'ENERGY'
            when upper(sector) like '%REAL ESTATE%' then 'REAL_ESTATE'
            else 'OTHER'
        end as standardized_sector,
        
        -- Geographic standardization
        case 
            when upper(geography) like '%NORTH AMERICA%' or upper(geography) like '%US%' or upper(geography) like '%CANADA%' then 'NORTH_AMERICA'
            when upper(geography) like '%EUROPE%' or upper(geography) like '%EU%' then 'EUROPE'
            when upper(geography) like '%ASIA%' or upper(geography) like '%PACIFIC%' then 'ASIA_PACIFIC'
            when upper(geography) like '%LATIN%' or upper(geography) like '%SOUTH AMERICA%' then 'LATIN_AMERICA'
            else 'OTHER'
        end as standardized_geography,
        
        -- Performance categorization
        case 
            when unrealized_return_multiple >= 3.0 then 'EXCELLENT'
            when unrealized_return_multiple >= 2.0 then 'GOOD'
            when unrealized_return_multiple >= 1.0 then 'FAIR'
            when unrealized_return_multiple >= 0.0 then 'POOR'
            when unrealized_return_multiple < 0.0 then 'LOSS'
            else 'UNKNOWN'
        end as performance_category,
        
        -- Ownership influence assessment
        case 
            when ownership_percentage >= 50 then 'MAJORITY'
            when ownership_percentage >= 25 then 'SIGNIFICANT_MINORITY'
            when ownership_percentage >= 10 then 'MINORITY'
            when ownership_percentage > 0 then 'SMALL_STAKE'
            else 'UNKNOWN'
        end as ownership_category,
        
        -- Governance influence
        case 
            when board_seats >= 2 then 'STRONG_GOVERNANCE'
            when board_seats = 1 then 'BOARD_REPRESENTATION'
            when board_seats = 0 then 'NO_BOARD_SEATS'
            else 'UNKNOWN'
        end as governance_influence,
        
        -- Time-based metrics
        quarter(valuation_date) as valuation_quarter,
        year(valuation_date) as valuation_year

    from cleaned
),

final as (
    select
        *,
        
        -- Overall investment quality assessment
        case 
            when performance_category in ('EXCELLENT', 'GOOD') 
                and standardized_valuation_method in ('MARKET_MULTIPLE', 'RECENT_TRANSACTION')
            then 'HIGH_QUALITY'
            when performance_category in ('EXCELLENT', 'GOOD', 'FAIR') 
                and standardized_valuation_method != 'OTHER'
            then 'MEDIUM_QUALITY'
            else 'LOW_QUALITY'
        end as investment_quality_rating,
        
        -- Validation flag for unrealized gain/loss
        case 
            when abs(coalesce(unrealized_gain_loss, 0) - coalesce(calculated_unrealized_gain_loss, 0)) > 1
            then true
            else false
        end as unrealized_calculation_variance,
        
        -- Data completeness assessment
        (
            case when investment_name is not null then 1 else 0 end +
            case when valuation_date is not null then 1 else 0 end +
            case when cost_basis is not null then 1 else 0 end +
            case when fair_value is not null then 1 else 0 end +
            case when valuation_method is not null then 1 else 0 end +
            case when investment_date is not null then 1 else 0 end +
            case when investment_stage is not null then 1 else 0 end +
            case when sector is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        hash(
            nav_investment_id,
            fund_code,
            investment_id,
            valuation_date,
            cost_basis,
            fair_value,
            valuation_method,
            valuation_multiple,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final