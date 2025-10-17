{{
  config(
    materialized='view',
    tags=['portfolio_mgmt', 'staging']
  )
}}

/*
  Staging model for portfolio management investments data
  
  This model cleans and standardizes investment data from the portfolio management system,
  handling investment terms, governance rights, and strategic information.
  
  Transformations applied:
  - Validate and standardize financial amounts
  - Parse and validate dates
  - Standardize investment types and stages
  - Handle governance and legal terms
  - Clean strategic information (thesis, risks, exit strategy)
  - Add investment categorization and analysis
*/

with source as (
    select * from {{ source('portfolio_management_vendor', 'amos_pm_investments') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(investment_id) as investment_id,
        trim(company_id) as company_id,
        trim(fund_id) as fund_id,
        
        -- Investment details
        trim(company_name) as company_name,
        trim(fund_name) as fund_name,
        
        -- Investment date
        case 
            when investment_date is not null 
            then cast(investment_date as date)
            else null
        end as investment_date,
        
        -- Financial amounts
        case 
            when initial_investment_amount is not null and initial_investment_amount > 0
            then cast(initial_investment_amount as number(20,2))
            else null
        end as initial_investment_amount,
        
        upper(trim(initial_investment_currency)) as initial_investment_currency,
        
        case 
            when total_invested_amount is not null and total_invested_amount > 0
            then cast(total_invested_amount as number(20,2))
            else null
        end as total_invested_amount,
        
        upper(trim(total_invested_currency)) as total_invested_currency,
        
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
        
        -- Investment classification
        trim(investment_type) as investment_type,
        trim(investment_stage) as investment_stage,
        trim(sector) as sector,
        trim(geography) as geography,
        
        -- Legal terms and rights
        trim(liquidation_preference) as liquidation_preference,
        trim(anti_dilution_protection) as anti_dilution_protection,
        
        case 
            when upper(trim(drag_along_rights)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(drag_along_rights)) in ('NO', 'FALSE', '0', 'N') then false
            else null
        end as has_drag_along_rights,
        
        case 
            when upper(trim(tag_along_rights)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(tag_along_rights)) in ('NO', 'FALSE', '0', 'N') then false
            else null
        end as has_tag_along_rights,
        
        -- Strategic information
        trim(investment_thesis) as investment_thesis,
        trim(key_risks) as key_risks,
        trim(exit_strategy) as exit_strategy,
        
        case 
            when target_exit_date is not null 
            then cast(target_exit_date as date)
            else null
        end as target_exit_date,
        
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
        'amos_pm_investments' as source_table,
        current_timestamp() as loaded_at

    from source
    where investment_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when initial_investment_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then initial_investment_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_initial_currency,
        
        case 
            when total_invested_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then total_invested_currency
            else initial_investment_currency  -- Default to initial currency
        end as validated_total_currency,
        
        -- Calculate follow-on investment
        case 
            when total_invested_amount is not null and initial_investment_amount is not null
            then total_invested_amount - initial_investment_amount
            else null
        end as follow_on_investment_amount,
        
        -- Investment type standardization
        case 
            when upper(investment_type) like '%EQUITY%' or upper(investment_type) like '%STOCK%' then 'EQUITY'
            when upper(investment_type) like '%DEBT%' or upper(investment_type) like '%LOAN%' then 'DEBT'
            when upper(investment_type) like '%CONVERTIBLE%' then 'CONVERTIBLE'
            when upper(investment_type) like '%WARRANT%' then 'WARRANT'
            when upper(investment_type) like '%MEZZANINE%' then 'MEZZANINE'
            else 'OTHER'
        end as standardized_investment_type,
        
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
        
        -- Ownership categorization
        case 
            when ownership_percentage >= 50 then 'MAJORITY'
            when ownership_percentage >= 25 then 'SIGNIFICANT_MINORITY'
            when ownership_percentage >= 10 then 'MINORITY'
            when ownership_percentage > 0 then 'SMALL_STAKE'
            else 'UNKNOWN'
        end as ownership_category,
        
        -- Governance influence assessment
        case 
            when board_seats >= 2 then 'STRONG_GOVERNANCE'
            when board_seats = 1 then 'BOARD_REPRESENTATION'
            when board_seats = 0 then 'NO_BOARD_SEATS'
            else 'UNKNOWN'
        end as governance_influence,
        
        -- Liquidation preference parsing
        case 
            when liquidation_preference like '%Non-Participating%' then 'NON_PARTICIPATING'
            when liquidation_preference like '%Participating%' then 'PARTICIPATING'
            when liquidation_preference like '%1.0x%' then 'SIMPLE_PREFERRED'
            else 'OTHER'
        end as liquidation_preference_type,
        
        -- Anti-dilution protection standardization
        case 
            when upper(anti_dilution_protection) like '%WEIGHTED AVERAGE BROAD%' then 'WEIGHTED_AVERAGE_BROAD'
            when upper(anti_dilution_protection) like '%WEIGHTED AVERAGE NARROW%' then 'WEIGHTED_AVERAGE_NARROW'
            when upper(anti_dilution_protection) like '%FULL RATCHET%' then 'FULL_RATCHET'
            when upper(anti_dilution_protection) like '%NO%' or upper(anti_dilution_protection) like '%NONE%' then 'NONE'
            else 'OTHER'
        end as standardized_anti_dilution,
        
        -- Exit strategy standardization
        case 
            when upper(exit_strategy) like '%IPO%' then 'IPO'
            when upper(exit_strategy) like '%STRATEGIC%' then 'STRATEGIC_SALE'
            when upper(exit_strategy) like '%FINANCIAL%' or upper(exit_strategy) like '%SPONSOR%' then 'FINANCIAL_SALE'
            when upper(exit_strategy) like '%MANAGEMENT%' or upper(exit_strategy) like '%MBO%' then 'MANAGEMENT_BUYOUT'
            when upper(exit_strategy) like '%DIVIDEND%' then 'DIVIDEND_RECAP'
            else 'OTHER'
        end as standardized_exit_strategy,
        
        -- Investment age calculation
        case 
            when investment_date is not null
            then datediff('month', investment_date, current_date())
            else null
        end as investment_age_months,
        
        case 
            when investment_date is not null
            then datediff('year', investment_date, current_date())
            else null
        end as investment_age_years,
        
        -- Target holding period
        case 
            when investment_date is not null and target_exit_date is not null
            then datediff('year', investment_date, target_exit_date)
            else null
        end as target_holding_period_years,
        
        -- Investment size categorization
        case 
            when total_invested_amount >= 100000000 then 'LARGE_CAP'
            when total_invested_amount >= 25000000 then 'MID_CAP'
            when total_invested_amount >= 5000000 then 'SMALL_CAP'
            when total_invested_amount > 0 then 'MICRO_CAP'
            else 'UNKNOWN'
        end as investment_size_category

    from cleaned
),

final as (
    select
        *,
        
        -- Overall investment profile assessment
        case 
            when ownership_category in ('MAJORITY', 'SIGNIFICANT_MINORITY') 
                and governance_influence in ('STRONG_GOVERNANCE', 'BOARD_REPRESENTATION')
                and standardized_investment_stage in ('GROWTH', 'BUYOUT')
            then 'CONTROL_ORIENTED'
            when ownership_category in ('MINORITY', 'SMALL_STAKE') 
                and standardized_investment_stage in ('SEED', 'EARLY_STAGE', 'GROWTH')
            then 'GROWTH_ORIENTED'
            when standardized_investment_stage = 'DISTRESSED'
            then 'SPECIAL_SITUATIONS'
            else 'OTHER'
        end as investment_profile,
        
        -- Risk assessment based on stage and sector
        case 
            when standardized_investment_stage in ('SEED', 'EARLY_STAGE') then 'HIGH_RISK'
            when standardized_investment_stage = 'GROWTH' and standardized_sector = 'TECHNOLOGY' then 'MEDIUM_HIGH_RISK'
            when standardized_investment_stage in ('GROWTH', 'LATE_STAGE') then 'MEDIUM_RISK'
            when standardized_investment_stage = 'BUYOUT' then 'MEDIUM_LOW_RISK'
            when standardized_investment_stage = 'DISTRESSED' then 'HIGH_RISK'
            else 'UNKNOWN'
        end as risk_assessment,
        
        -- Data completeness assessment
        (
            case when company_name is not null then 1 else 0 end +
            case when investment_date is not null then 1 else 0 end +
            case when total_invested_amount is not null then 1 else 0 end +
            case when ownership_percentage is not null then 1 else 0 end +
            case when investment_stage is not null then 1 else 0 end +
            case when sector is not null then 1 else 0 end +
            case when exit_strategy is not null then 1 else 0 end +
            case when investment_thesis is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        hash(
            investment_id,
            company_id,
            fund_id,
            investment_date,
            total_invested_amount,
            ownership_percentage,
            investment_stage,
            sector,
            exit_strategy,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final