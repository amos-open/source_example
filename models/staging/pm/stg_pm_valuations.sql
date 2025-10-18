{{
  config(
    materialized='view',
    tags=['portfolio_mgmt', 'staging']
  )
}}

/*
  Staging model for portfolio management valuations data
  
  This model cleans and standardizes valuation data from the portfolio management system,
  handling valuation methodologies, multiples, and confidence assessments.
  
  Transformations applied:
  - Validate and standardize valuation amounts
  - Parse and validate dates
  - Standardize valuation methods and confidence levels
  - Handle valuation multiples and ratios
  - Clean comparable company data
  - Add valuation trend analysis and quality assessment
*/

with source as (
    select * from {{ source('portfolio_management_vendor', 'amos_pm_valuations') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(valuation_id) as valuation_id,
        trim(company_id) as company_id,
        trim(investment_id) as investment_id,
        trim(company_name) as company_name,
        
        -- Valuation date
        case 
            when valuation_date is not null 
            then CAST(valuation_date AS DATE)
            else null
        end as valuation_date,
        
        -- Valuation methodology
        trim(valuation_method) as valuation_method,
        
        -- Enterprise and equity values
        case 
            when enterprise_value is not null and enterprise_value > 0
            then CAST(enterprise_value AS NUMERIC(24,2))
            else null
        end as enterprise_value,
        
        upper(trim(enterprise_value_currency)) as enterprise_value_currency,
        
        case 
            when equity_value is not null and equity_value > 0
            then CAST(equity_value AS NUMERIC(24,2))
            else null
        end as equity_value,
        
        upper(trim(equity_value_currency)) as equity_value_currency,
        
        -- Valuation multiples
        case 
            when revenue_multiple is not null and revenue_multiple > 0
            then CAST(revenue_multiple AS NUMERIC(8,2))
            else null
        end as revenue_multiple,
        
        case 
            when ebitda_multiple is not null and ebitda_multiple > 0
            then CAST(ebitda_multiple AS NUMERIC(8,2))
            else null
        end as ebitda_multiple,
        
        case 
            when book_multiple is not null and book_multiple > 0
            then CAST(book_multiple AS NUMERIC(8,2))
            else null
        end as book_multiple,
        
        -- DCF parameters
        case 
            when discount_rate is not null 
                and discount_rate between 0 and 1
            then CAST(discount_rate AS NUMERIC(8,6))
            else null
        end as discount_rate,
        
        case 
            when terminal_growth_rate is not null 
                and terminal_growth_rate between -0.1 and 0.2
            then CAST(terminal_growth_rate AS NUMERIC(8,6))
            else null
        end as terminal_growth_rate,
        
        -- Comparable companies and benchmarking
        trim(comparable_companies) as comparable_companies,
        
        -- Valuation metadata
        trim(valuation_notes) as valuation_notes,
        trim(valuation_source) as valuation_source,
        trim(confidence_level) as confidence_level,
        
        -- Last financing reference
        case 
            when last_financing_round_valuation is not null and last_financing_round_valuation > 0
            then CAST(last_financing_round_valuation AS NUMERIC(24,2))
            else null
        end as last_financing_round_valuation,
        
        case 
            when last_financing_date is not null 
            then CAST(last_financing_date AS DATE)
            else null
        end as last_financing_date,
        
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
        'PORTFOLIO_MGMT_VENDOR' as source_system,
        'amos_pm_valuations' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where valuation_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when enterprise_value_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then enterprise_value_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_enterprise_currency,
        
        case 
            when equity_value_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then equity_value_currency
            else enterprise_value_currency  -- Default to enterprise currency
        end as validated_equity_currency,
        
        -- Valuation method standardization
        case 
            when upper(valuation_method) like '%MARKET%' 
                or upper(valuation_method) like '%MULTIPLE%' 
                or upper(valuation_method) like '%COMPARABLE%' then 'MARKET_MULTIPLE'
            when upper(valuation_method) like '%DCF%' 
                or upper(valuation_method) like '%DISCOUNTED%' 
                or upper(valuation_method) like '%CASH FLOW%' then 'DCF'
            when upper(valuation_method) like '%TRANSACTION%' 
                or upper(valuation_method) like '%PRECEDENT%' then 'PRECEDENT_TRANSACTION'
            when upper(valuation_method) like '%ASSET%' 
                or upper(valuation_method) like '%BOOK%' then 'ASSET_BASED'
            when upper(valuation_method) like '%COST%' then 'COST_APPROACH'
            when upper(valuation_method) like '%LIQUIDATION%' then 'LIQUIDATION_VALUE'
            else 'OTHER'
        end as standardized_valuation_method,
        
        -- Confidence level standardization
        case 
            when upper(confidence_level) in ('HIGH', 'STRONG', 'CONFIDENT') then 'HIGH'
            when upper(confidence_level) in ('MEDIUM', 'MODERATE', 'FAIR') then 'MEDIUM'
            when upper(confidence_level) in ('LOW', 'WEAK', 'UNCERTAIN') then 'LOW'
            else 'UNKNOWN'
        end as standardized_confidence_level,
        
        -- Valuation source standardization
        case 
            when upper(valuation_source) like '%EXTERNAL%' 
                or upper(valuation_source) like '%THIRD PARTY%' 
                or upper(valuation_source) like '%INDEPENDENT%' then 'EXTERNAL'
            when upper(valuation_source) like '%INTERNAL%' 
                or upper(valuation_source) like '%MANAGEMENT%' then 'INTERNAL'
            when upper(valuation_source) like '%BOARD%' then 'BOARD_APPROVED'
            else 'UNKNOWN'
        end as standardized_valuation_source,
        
        -- Calculate implied net debt (enterprise value - equity value)
        case 
            when enterprise_value is not null and equity_value is not null
            then enterprise_value - equity_value
            else null
        end as implied_net_debt,
        
        -- Valuation vs last financing analysis
        case 
            when equity_value is not null and last_financing_round_valuation is not null and last_financing_round_valuation > 0
            then (equity_value / last_financing_round_valuation) - 1
            else null
        end as valuation_change_vs_last_financing,
        
        case 
            when valuation_date is not null and last_financing_date is not null
            then DATEDIFF('month', last_financing_date, valuation_date)
            else null
        end as months_since_last_financing,
        
        -- Multiple categorization
        case 
            when revenue_multiple >= 20 then 'VERY_HIGH_REVENUE_MULTIPLE'
            when revenue_multiple >= 10 then 'HIGH_REVENUE_MULTIPLE'
            when revenue_multiple >= 5 then 'MODERATE_REVENUE_MULTIPLE'
            when revenue_multiple >= 2 then 'LOW_REVENUE_MULTIPLE'
            when revenue_multiple > 0 then 'VERY_LOW_REVENUE_MULTIPLE'
            else 'NO_REVENUE_MULTIPLE'
        end as revenue_multiple_category,
        
        case 
            when ebitda_multiple >= 50 then 'VERY_HIGH_EBITDA_MULTIPLE'
            when ebitda_multiple >= 25 then 'HIGH_EBITDA_MULTIPLE'
            when ebitda_multiple >= 15 then 'MODERATE_EBITDA_MULTIPLE'
            when ebitda_multiple >= 8 then 'LOW_EBITDA_MULTIPLE'
            when ebitda_multiple > 0 then 'VERY_LOW_EBITDA_MULTIPLE'
            else 'NO_EBITDA_MULTIPLE'
        end as ebitda_multiple_category,
        
        -- Valuation reasonableness checks
        case 
            when enterprise_value is not null and equity_value is not null then
                case 
                    when enterprise_value < equity_value then 'NEGATIVE_NET_DEBT'
                    when enterprise_value > equity_value * 3 then 'HIGH_NET_DEBT'
                    else 'REASONABLE'
                end
            else 'INSUFFICIENT_DATA'
        end as enterprise_equity_relationship,
        
        -- Time-based metrics
        quarter(valuation_date) as valuation_quarter,
        year(valuation_date) as valuation_year,
        
        -- Count comparable companies
        case 
            when comparable_companies is not null and comparable_companies != ''
            then array_size(split(comparable_companies, ','))
            else 0
        end as comparable_companies_count,
        
        -- Valuation vintage assessment
        case 
            when valuation_date >= current_date() - interval '3 months' then 'CURRENT'
            when valuation_date >= current_date() - interval '6 months' then 'RECENT'
            when valuation_date >= current_date() - interval '12 months' then 'STALE'
            else 'VERY_STALE'
        end as valuation_vintage

    from cleaned
),

final as (
    select
        *,
        
        -- Overall valuation quality assessment
        case 
            when standardized_valuation_method in ('MARKET_MULTIPLE', 'DCF') 
                and standardized_confidence_level = 'HIGH'
                and standardized_valuation_source = 'EXTERNAL'
                and valuation_vintage in ('CURRENT', 'RECENT')
            then 'HIGH_QUALITY'
            when standardized_valuation_method in ('MARKET_MULTIPLE', 'DCF', 'PRECEDENT_TRANSACTION') 
                and standardized_confidence_level in ('HIGH', 'MEDIUM')
                and valuation_vintage in ('CURRENT', 'RECENT')
            then 'MEDIUM_QUALITY'
            when standardized_valuation_method != 'OTHER'
                and valuation_vintage != 'VERY_STALE'
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as valuation_quality_rating,
        
        -- Valuation trend assessment (requires multiple periods - placeholder)
        case 
            when valuation_change_vs_last_financing > 0.5 then 'STRONG_APPRECIATION'
            when valuation_change_vs_last_financing > 0.2 then 'MODERATE_APPRECIATION'
            when valuation_change_vs_last_financing > -0.1 then 'STABLE'
            when valuation_change_vs_last_financing > -0.3 then 'MODERATE_DECLINE'
            else 'SIGNIFICANT_DECLINE'
        end as valuation_trend_vs_financing,
        
        -- Data completeness assessment
        (
            case when enterprise_value is not null then 1 else 0 end +
            case when equity_value is not null then 1 else 0 end +
            case when valuation_method is not null then 1 else 0 end +
            case when confidence_level is not null then 1 else 0 end +
            case when valuation_source is not null then 1 else 0 end +
            case when (revenue_multiple is not null or ebitda_multiple is not null) then 1 else 0 end +
            case when comparable_companies is not null then 1 else 0 end +
            case when valuation_notes is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        TO_VARCHAR(MD5(CONCAT(
          COALESCE(valuation_id,''),
          COALESCE(company_id,''),
          COALESCE(investment_id,''),
          COALESCE(TO_VARCHAR(valuation_date),''),
          COALESCE(TO_VARCHAR(enterprise_value),''),
          COALESCE(TO_VARCHAR(equity_value),''),
          COALESCE(valuation_method,''),
          COALESCE(TO_VARCHAR(revenue_multiple),''),
          COALESCE(TO_VARCHAR(ebitda_multiple),''),
          COALESCE(confidence_level,''),
          COALESCE(TO_VARCHAR(last_modified_date),'')
        ))) as record_hash

    from enhanced
)

select * from final