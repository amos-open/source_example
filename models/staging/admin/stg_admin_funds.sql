{{
  config(
    materialized='view',
    tags=['fund_admin', 'staging']
  )
}}

/*
  Staging model for fund administration funds data
  
  This model cleans and standardizes fund master data from the fund administration system,
  handling financial amounts, dates, and fund terms appropriately.
  
  Transformations applied:
  - Standardize fund names and legal names
  - Validate and convert financial amounts
  - Parse and validate dates
  - Standardize currency codes
  - Calculate derived fund metrics
  - Handle fund status and lifecycle information
*/

with source as (
    select * from {{ source('fund_admin_vendor', 'amos_admin_funds') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(fund_code) as fund_code,
        
        -- Fund names
        trim(fund_name) as fund_name,
        trim(fund_legal_name) as fund_legal_name,
        
        -- Fund sizing and vintage
        case 
            when vintage_year is not null 
                and vintage_year between 1990 and EXTRACT(YEAR FROM CURRENT_DATE()) + 2
            then CAST(vintage_year AS NUMERIC(4,0))
            else null
        end as vintage_year,
        
        case 
            when target_size is not null and target_size > 0
            then CAST(target_size AS NUMERIC(20,2))
            else null
        end as target_size,
        
        upper(trim(target_size_currency)) as target_size_currency,
        
        case 
            when final_size is not null and final_size > 0
            then CAST(final_size AS NUMERIC(20,2))
            else null
        end as final_size,
        
        upper(trim(final_size_currency)) as final_size_currency,
        upper(trim(base_currency_code)) as base_currency_code,
        
        -- Fund classification
        trim(fund_type) as fund_type,
        trim(strategy) as investment_strategy,
        trim(geography_focus) as geography_focus,
        trim(sector_focus) as sector_focus,
        
        -- Investment period
        case 
            when investment_period_start is not null 
            then CAST(investment_period_start AS DATE)
            else null
        end as investment_period_start,
        
        case 
            when investment_period_end is not null 
            then CAST(investment_period_end AS DATE)
            else null
        end as investment_period_end,
        
        case 
            when fund_life_years is not null 
                and fund_life_years between 5 and 20
            then CAST(fund_life_years AS NUMERIC(2,0))
            else null
        end as fund_life_years,
        
        -- Fee structure
        case 
            when management_fee_rate is not null 
                and management_fee_rate between 0 and 0.05
            then CAST(management_fee_rate AS NUMERIC(8,6))
            else null
        end as management_fee_rate,
        
        case 
            when carried_interest_rate is not null 
                and carried_interest_rate between 0 and 0.50
            then CAST(carried_interest_rate AS NUMERIC(8,6))
            else null
        end as carried_interest_rate,
        
        case 
            when hurdle_rate is not null 
                and hurdle_rate between 0 and 0.20
            then CAST(hurdle_rate AS NUMERIC(8,6))
            else null
        end as hurdle_rate,
        
        case 
            when upper(trim(catch_up_provision)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(catch_up_provision)) in ('NO', 'FALSE', '0', 'N') then false
            else null
        end as has_catch_up_provision,
        
        trim(distribution_policy) as distribution_policy,
        
        -- Fund status and dates
        upper(trim(status)) as fund_status,
        
        case 
            when first_close_date is not null 
            then CAST(first_close_date AS DATE)
            else null
        end as first_close_date,
        
        case 
            when final_close_date is not null 
            then CAST(final_close_date AS DATE)
            else null
        end as final_close_date,
        
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
        'amos_admin_funds' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where fund_code is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when base_currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then base_currency_code
            else 'USD'  -- Default to USD for invalid codes
        end as validated_base_currency,
        
        -- Calculate fund size variance
        case 
            when target_size is not null and final_size is not null and target_size > 0
            then (final_size - target_size) / target_size * 100
            else null
        end as size_variance_percentage,
        
        -- Fund lifecycle stage
        case 
            when fund_status = 'ACTIVE' and investment_period_end >= current_date()
            then 'INVESTMENT_PERIOD'
            when fund_status = 'ACTIVE' and investment_period_end < current_date()
            then 'HARVEST_PERIOD'
            when fund_status = 'CLOSED'
            then 'LIQUIDATED'
            when fund_status = 'FUNDRAISING'
            then 'FUNDRAISING'
            else 'UNKNOWN'
        end as lifecycle_stage,
        
        -- Investment period duration in years
        case 
            when investment_period_start is not null and investment_period_end is not null
            then DATEDIFF('year', investment_period_start, investment_period_end)
            else null
        end as investment_period_years,
        
        -- Fund age in years from first close
        case 
            when first_close_date is not null
            then DATEDIFF('year', first_close_date, CURRENT_DATE())
            else null
        end as fund_age_years,
        
        -- Fundraising duration in days
        case 
            when first_close_date is not null and final_close_date is not null
            then DATEDIFF('day', first_close_date, final_close_date)
            else null
        end as fundraising_duration_days,
        
        -- Fee structure categorization
        case 
            when management_fee_rate >= 0.025 then 'HIGH'
            when management_fee_rate >= 0.015 then 'STANDARD'
            when management_fee_rate > 0 then 'LOW'
            else 'UNKNOWN'
        end as management_fee_category,
        
        case 
            when carried_interest_rate >= 0.25 then 'HIGH'
            when carried_interest_rate >= 0.15 then 'STANDARD'
            when carried_interest_rate > 0 then 'LOW'
            else 'UNKNOWN'
        end as carry_category,
        
        -- Data completeness score
        (
            case when fund_name is not null then 1 else 0 end +
            case when vintage_year is not null then 1 else 0 end +
            case when target_size is not null then 1 else 0 end +
            case when base_currency_code is not null then 1 else 0 end +
            case when investment_strategy is not null then 1 else 0 end +
            case when management_fee_rate is not null then 1 else 0 end +
            case when carried_interest_rate is not null then 1 else 0 end +
            case when first_close_date is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score

    from cleaned
),

final as (
    select
        *,
        
        -- Overall fund data quality
        case 
            when completeness_score >= 90 then 'HIGH'
            when completeness_score >= 70 then 'MEDIUM'
            else 'LOW'
        end as data_quality_rating,
        
        -- Record hash for change detection
        TO_VARCHAR(MD5(CONCAT(
          COALESCE(fund_code,''),
          COALESCE(fund_name,''),
          COALESCE(TO_VARCHAR(vintage_year),''),
          COALESCE(TO_VARCHAR(target_size),''),
          COALESCE(TO_VARCHAR(final_size),''),
          COALESCE(base_currency_code,''),
          COALESCE(investment_strategy,''),
          COALESCE(TO_VARCHAR(management_fee_rate),''),
          COALESCE(TO_VARCHAR(carried_interest_rate),''),
          COALESCE(fund_status,''),
          COALESCE(TO_VARCHAR(last_modified_date),'')
        ))) as record_hash

    from enhanced
)

select * from final