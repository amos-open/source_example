{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging model for reference currencies data
  
  This model cleans and standardizes currency reference data,
  providing a master list of currencies used across the system.
  
  Transformations applied:
  - Validate currency codes against ISO 4217 standards
  - Standardize currency names and symbols
  - Handle regional classifications
  - Add currency usage and validation flags
*/

with source as (
    select * from {{ source('reference', 'amos_ref_currencies') }}
),

cleaned as (
    select
        -- Primary identifier
        upper(trim(currency_code)) as currency_code,
        
        -- Currency details
        trim(currency_name) as currency_name,
        trim(currency_symbol) as currency_symbol,
        
        case 
            when numeric_code is not null 
                and numeric_code between 1 and 999
            then CAST(numeric_code AS NUMERIC(3,0))
            else null
        end as iso_numeric_code,
        
        case 
            when minor_unit is not null 
                and minor_unit between 0 and 4
            then CAST(minor_unit AS NUMERIC(1,0))
            else 2  -- Default to 2 decimal places
        end as decimal_places,
        
        case 
            when upper(trim(is_active)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(is_active)) in ('NO', 'FALSE', '0', 'N') then false
            else true  -- Default to active
        end as is_active,
        
        trim(region) as region,
        
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
        'REFERENCE' as source_system,
        'amos_ref_currencies' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where currency_code is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Currency validation against common ISO 4217 codes
        case 
            when currency_code in (
                'USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD', 'NZD', 'SEK', 'NOK', 'DKK',
                'SGD', 'HKD', 'CNY', 'INR', 'KRW', 'BRL', 'MXN', 'ZAR', 'RUB', 'TRY', 'PLN',
                'CZK', 'HUF', 'ILS', 'AED', 'SAR', 'QAR', 'KWD', 'BHD', 'OMR', 'JOD', 'LBP',
                'EGP', 'MAD', 'TND', 'DZD', 'LYD', 'NGN', 'GHS', 'KES', 'UGX', 'TZS', 'ZMW',
                'BWP', 'MUR', 'SCR', 'MGA', 'KMF', 'SZL', 'LSL', 'NAD', 'AOA', 'MZN', 'ZWL'
            ) then true
            else false
        end as is_iso_standard_currency,
        
        -- Major currency classification
        case 
            when currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CHF', 'CAD', 'AUD') then 'MAJOR'
            when currency_code in ('SEK', 'NOK', 'DKK', 'SGD', 'HKD', 'NZD') then 'MINOR'
            when currency_code in ('CNY', 'INR', 'KRW', 'BRL', 'MXN', 'ZAR', 'RUB') then 'EMERGING'
            else 'OTHER'
        end as currency_tier,
        
        -- Regional standardization
        case 
            when upper(region) like '%NORTH AMERICA%' or upper(region) like '%AMERICAS%' then 'NORTH_AMERICA'
            when upper(region) like '%EUROPE%' then 'EUROPE'
            when upper(region) like '%ASIA%' or upper(region) like '%PACIFIC%' then 'ASIA_PACIFIC'
            when upper(region) like '%MIDDLE EAST%' then 'MIDDLE_EAST'
            when upper(region) like '%AFRICA%' then 'AFRICA'
            when upper(region) like '%LATIN%' or upper(region) like '%SOUTH AMERICA%' then 'LATIN_AMERICA'
            else 'OTHER'
        end as standardized_region,
        
        -- Currency usage in private markets
        case 
            when currency_code in ('USD', 'EUR', 'GBP') then 'PRIMARY'
            when currency_code in ('JPY', 'CHF', 'CAD', 'AUD', 'SGD', 'HKD') then 'SECONDARY'
            when currency_code in ('CNY', 'INR', 'KRW', 'BRL') then 'EMERGING_MARKETS'
            else 'SPECIALTY'
        end as private_markets_usage,
        
        -- Volatility classification (simplified)
        case 
            when currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CHF') then 'LOW_VOLATILITY'
            when currency_code in ('CAD', 'AUD', 'SEK', 'NOK', 'SGD') then 'MEDIUM_VOLATILITY'
            when currency_code in ('BRL', 'MXN', 'ZAR', 'RUB', 'TRY') then 'HIGH_VOLATILITY'
            else 'UNKNOWN_VOLATILITY'
        end as volatility_classification,
        
        -- Liquidity assessment
        case 
            when currency_code in ('USD', 'EUR', 'GBP', 'JPY') then 'HIGHLY_LIQUID'
            when currency_code in ('CHF', 'CAD', 'AUD', 'SEK', 'NOK') then 'LIQUID'
            when currency_code in ('SGD', 'HKD', 'NZD', 'DKK') then 'MODERATELY_LIQUID'
            else 'LIMITED_LIQUIDITY'
        end as liquidity_assessment,
        
        -- Decimal places validation
        case 
            when currency_code = 'JPY' and decimal_places != 0 then 'INCORRECT_DECIMALS'
            when currency_code in ('USD', 'EUR', 'GBP', 'CAD', 'AUD') and decimal_places != 2 then 'INCORRECT_DECIMALS'
            else 'CORRECT_DECIMALS'
        end as decimal_validation_status

    from cleaned
),

final as (
    select
        *,
        
        -- Overall currency data quality
        case 
            when is_iso_standard_currency = true 
                and decimal_validation_status = 'CORRECT_DECIMALS'
                and currency_name is not null
                and currency_symbol is not null
            then 'HIGH_QUALITY'
            when is_iso_standard_currency = true 
                and currency_name is not null
            then 'MEDIUM_QUALITY'
            when currency_code is not null
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as data_quality_rating,
        
        -- Recommended for use in private markets
        case 
            when private_markets_usage in ('PRIMARY', 'SECONDARY')
                and liquidity_assessment in ('HIGHLY_LIQUID', 'LIQUID')
                and is_active = true
            then true
            else false
        end as recommended_for_private_markets,
        
        -- Completeness score
        (
            case when currency_name is not null then 1 else 0 end +
            case when currency_symbol is not null then 1 else 0 end +
            case when iso_numeric_code is not null then 1 else 0 end +
            case when decimal_places is not null then 1 else 0 end +
            case when region is not null then 1 else 0 end
        ) / 5.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(currency_code, currency_name, currency_symbol, iso_numeric_code, decimal_places, is_active, region, last_modified_date)) as record_hash

    from enhanced
)

select * from final