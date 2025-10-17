{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging model for reference FX rates data
  
  This model cleans and standardizes foreign exchange rate data,
  providing historical currency conversion rates for financial calculations.
  
  Transformations applied:
  - Validate currency codes and exchange rates
  - Standardize rate sources and dates
  - Handle rate reasonableness checks
  - Add rate change analysis and validation flags
*/

with source as (
    select * from {{ source('reference', 'amos_ref_fx_rates') }}
),

cleaned as (
    select
        -- Rate date
        case 
            when rate_date is not null 
            then cast(rate_date as date)
            else null
        end as rate_date,
        
        -- Currency pair
        upper(trim(base_currency)) as base_currency_code,
        upper(trim(quote_currency)) as quote_currency_code,
        
        -- Exchange rate
        case 
            when exchange_rate is not null and exchange_rate > 0
            then cast(exchange_rate as number(18,8))
            else null
        end as exchange_rate,
        
        -- Rate metadata
        trim(rate_source) as rate_source,
        
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
        'REFERENCE' as source_system,
        'amos_ref_fx_rates' as source_table,
        current_timestamp() as loaded_at

    from source
    where rate_date is not null 
        and base_currency is not null 
        and quote_currency is not null  -- Filter out incomplete records
),

enhanced as (
    select
        *,
        
        -- Currency code validation
        case 
            when base_currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'SEK', 'NOK', 'DKK', 'SGD', 'HKD', 'CNY', 'INR', 'KRW', 'BRL', 'MXN', 'ZAR', 'RUB')
            then true
            else false
        end as is_valid_base_currency,
        
        case 
            when quote_currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'SEK', 'NOK', 'DKK', 'SGD', 'HKD', 'CNY', 'INR', 'KRW', 'BRL', 'MXN', 'ZAR', 'RUB')
            then true
            else false
        end as is_valid_quote_currency,
        
        -- Currency pair standardization
        case 
            when base_currency_code < quote_currency_code 
            then base_currency_code || '/' || quote_currency_code
            else quote_currency_code || '/' || base_currency_code
        end as standardized_currency_pair,
        
        -- Inverse rate calculation
        case 
            when exchange_rate is not null and exchange_rate > 0
            then 1.0 / exchange_rate
            else null
        end as inverse_exchange_rate,
        
        -- Rate source standardization
        case 
            when upper(rate_source) like '%BLOOMBERG%' then 'BLOOMBERG'
            when upper(rate_source) like '%REUTERS%' then 'REUTERS'
            when upper(rate_source) like '%REFINITIV%' then 'REFINITIV'
            when upper(rate_source) like '%CENTRAL BANK%' or upper(rate_source) like '%FED%' then 'CENTRAL_BANK'
            when upper(rate_source) like '%ECB%' then 'ECB'
            when upper(rate_source) like '%BOE%' then 'BOE'
            when upper(rate_source) like '%BOJ%' then 'BOJ'
            when upper(rate_source) like '%OANDA%' then 'OANDA'
            when upper(rate_source) like '%XE%' then 'XE'
            else 'OTHER'
        end as standardized_rate_source,
        
        -- Rate reasonableness checks based on typical ranges
        case 
            when base_currency_code = 'USD' and quote_currency_code = 'EUR' 
                and (exchange_rate < 0.5 or exchange_rate > 1.5) then 'UNREASONABLE'
            when base_currency_code = 'USD' and quote_currency_code = 'GBP' 
                and (exchange_rate < 0.5 or exchange_rate > 1.2) then 'UNREASONABLE'
            when base_currency_code = 'USD' and quote_currency_code = 'JPY' 
                and (exchange_rate < 80 or exchange_rate > 200) then 'UNREASONABLE'
            when base_currency_code = 'USD' and quote_currency_code = 'CAD' 
                and (exchange_rate < 1.0 or exchange_rate > 1.8) then 'UNREASONABLE'
            when base_currency_code = 'USD' and quote_currency_code = 'CHF' 
                and (exchange_rate < 0.7 or exchange_rate > 1.3) then 'UNREASONABLE'
            when base_currency_code = 'USD' and quote_currency_code = 'SGD' 
                and (exchange_rate < 1.2 or exchange_rate > 1.8) then 'UNREASONABLE'
            when exchange_rate <= 0 then 'INVALID'
            else 'REASONABLE'
        end as rate_reasonableness_check,
        
        -- Major currency pair identification
        case 
            when (base_currency_code = 'USD' and quote_currency_code in ('EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD'))
                or (quote_currency_code = 'USD' and base_currency_code in ('EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD'))
            then 'MAJOR_PAIR'
            when (base_currency_code in ('EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD') 
                and quote_currency_code in ('EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD'))
                and base_currency_code != quote_currency_code
            then 'CROSS_PAIR'
            else 'EXOTIC_PAIR'
        end as currency_pair_type,
        
        -- Rate volatility classification (simplified)
        case 
            when currency_pair_type = 'MAJOR_PAIR' 
                and standardized_currency_pair in ('EUR/USD', 'GBP/USD', 'USD/JPY', 'USD/CHF') then 'LOW_VOLATILITY'
            when currency_pair_type = 'MAJOR_PAIR' then 'MEDIUM_VOLATILITY'
            when currency_pair_type = 'CROSS_PAIR' then 'MEDIUM_HIGH_VOLATILITY'
            else 'HIGH_VOLATILITY'
        end as expected_volatility,
        
        -- Time-based analysis
        year(rate_date) as rate_year,
        quarter(rate_date) as rate_quarter,
        month(rate_date) as rate_month,
        dayofweek(rate_date) as rate_day_of_week,
        
        -- Rate age assessment
        case 
            when rate_date >= current_date() - interval '1 day' then 'CURRENT'
            when rate_date >= current_date() - interval '7 days' then 'RECENT'
            when rate_date >= current_date() - interval '30 days' then 'STALE'
            else 'VERY_STALE'
        end as rate_age_assessment,
        
        -- Business day check (simplified - excludes weekends)
        case 
            when dayofweek(rate_date) in (1, 7) then 'WEEKEND'  -- Sunday = 1, Saturday = 7
            else 'BUSINESS_DAY'
        end as business_day_flag

    from cleaned
),

final as (
    select
        *,
        
        -- Overall rate data quality
        case 
            when is_valid_base_currency = true 
                and is_valid_quote_currency = true
                and rate_reasonableness_check = 'REASONABLE'
                and standardized_rate_source != 'OTHER'
                and rate_age_assessment in ('CURRENT', 'RECENT')
            then 'HIGH_QUALITY'
            when is_valid_base_currency = true 
                and is_valid_quote_currency = true
                and rate_reasonableness_check = 'REASONABLE'
                and rate_age_assessment in ('CURRENT', 'RECENT', 'STALE')
            then 'MEDIUM_QUALITY'
            when is_valid_base_currency = true 
                and is_valid_quote_currency = true
                and rate_reasonableness_check != 'INVALID'
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as data_quality_rating,
        
        -- Recommended for use in calculations
        case 
            when data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
                and currency_pair_type in ('MAJOR_PAIR', 'CROSS_PAIR')
                and business_day_flag = 'BUSINESS_DAY'
            then true
            else false
        end as recommended_for_calculations,
        
        -- Completeness score
        (
            case when base_currency_code is not null then 1 else 0 end +
            case when quote_currency_code is not null then 1 else 0 end +
            case when exchange_rate is not null then 1 else 0 end +
            case when rate_source is not null then 1 else 0 end +
            case when rate_date is not null then 1 else 0 end
        ) / 5.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        hash(
            rate_date,
            base_currency_code,
            quote_currency_code,
            exchange_rate,
            rate_source,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final