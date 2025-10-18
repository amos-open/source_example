{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging model for reference countries data
  
  This model cleans and standardizes country reference data,
  providing a master list of countries used across the system.
  
  Transformations applied:
  - Validate country codes against ISO 3166 standards
  - Standardize country names and regional classifications
  - Handle geographic and economic groupings
  - Add country usage and validation flags
*/

with source as (
    select * from {{ source('reference', 'amos_ref_countries') }}
),

cleaned as (
    select
        -- Primary identifier
        upper(trim(country_code)) as country_code,
        
        -- Country details
        trim(country_name) as country_name,
        upper(trim(iso_alpha_3)) as iso_alpha_3_code,
        
        case 
            when numeric_code is not null 
                and numeric_code between 1 and 999
            then CAST(numeric_code AS NUMERIC(3,0))
            else null
        end as iso_numeric_code,
        
        -- Geographic information
        trim(region) as region,
        trim(sub_region) as sub_region,
        trim(capital_city) as capital_city,
        
        -- Economic information
        upper(trim(currency_code)) as primary_currency_code,
        trim(phone_code) as international_phone_code,
        
        case 
            when upper(trim(is_active)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(is_active)) in ('NO', 'FALSE', '0', 'N') then false
            else true  -- Default to active
        end as is_active,
        
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
        'amos_ref_countries' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where country_code is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Country code validation against ISO 3166-1 alpha-2
        case 
            when length(country_code) = 2 
                and REGEXP_LIKE(country_code, '^[A-Z]{2}$') then true
            else false
        end as is_valid_iso_alpha_2,
        
        -- ISO alpha-3 validation
        case 
            when length(iso_alpha_3_code) = 3 
                and REGEXP_LIKE(iso_alpha_3_code, '^[A-Z]{3}$') then true
            else false
        end as is_valid_iso_alpha_3,
        
        -- Regional standardization
        case 
            when upper(region) like '%AMERICAS%' then 'AMERICAS'
            when upper(region) like '%EUROPE%' then 'EUROPE'
            when upper(region) like '%AFRICA%' then 'AFRICA'
            when upper(region) like '%ASIA%' then 'ASIA'
            when upper(region) like '%OCEANIA%' then 'OCEANIA'
            else 'OTHER'
        end as standardized_region,
        
        -- Sub-regional standardization
        case 
            when upper(sub_region) like '%NORTHERN AMERICA%' then 'NORTHERN_AMERICA'
            when upper(sub_region) like '%CENTRAL AMERICA%' then 'CENTRAL_AMERICA'
            when upper(sub_region) like '%SOUTH AMERICA%' then 'SOUTH_AMERICA'
            when upper(sub_region) like '%CARIBBEAN%' then 'CARIBBEAN'
            when upper(sub_region) like '%NORTHERN EUROPE%' then 'NORTHERN_EUROPE'
            when upper(sub_region) like '%WESTERN EUROPE%' then 'WESTERN_EUROPE'
            when upper(sub_region) like '%EASTERN EUROPE%' then 'EASTERN_EUROPE'
            when upper(sub_region) like '%SOUTHERN EUROPE%' then 'SOUTHERN_EUROPE'
            when upper(sub_region) like '%WESTERN ASIA%' then 'WESTERN_ASIA'
            when upper(sub_region) like '%CENTRAL ASIA%' then 'CENTRAL_ASIA'
            when upper(sub_region) like '%EASTERN ASIA%' then 'EASTERN_ASIA'
            when upper(sub_region) like '%SOUTH-EASTERN ASIA%' then 'SOUTHEASTERN_ASIA'
            when upper(sub_region) like '%SOUTHERN ASIA%' then 'SOUTHERN_ASIA'
            when upper(sub_region) like '%NORTHERN AFRICA%' then 'NORTHERN_AFRICA'
            when upper(sub_region) like '%WESTERN AFRICA%' then 'WESTERN_AFRICA'
            when upper(sub_region) like '%MIDDLE AFRICA%' then 'MIDDLE_AFRICA'
            when upper(sub_region) like '%EASTERN AFRICA%' then 'EASTERN_AFRICA'
            when upper(sub_region) like '%SOUTHERN AFRICA%' then 'SOUTHERN_AFRICA'
            when upper(sub_region) like '%AUSTRALIA%' then 'AUSTRALIA_NEW_ZEALAND'
            when upper(sub_region) like '%MELANESIA%' then 'MELANESIA'
            when upper(sub_region) like '%MICRONESIA%' then 'MICRONESIA'
            when upper(sub_region) like '%POLYNESIA%' then 'POLYNESIA'
            else 'OTHER'
        end as standardized_sub_region,
        
        -- Economic development classification (simplified)
        case 
            when country_code in (
                'US', 'CA', 'GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'BE', 'AT', 'CH', 'SE', 'NO', 'DK', 'FI',
                'JP', 'AU', 'NZ', 'SG', 'HK', 'KR', 'IL', 'IE', 'LU', 'IS', 'PT', 'GR', 'CZ', 'SI', 'SK',
                'EE', 'LV', 'LT', 'CY', 'MT'
            ) then 'DEVELOPED'
            when country_code in (
                'CN', 'IN', 'BR', 'MX', 'RU', 'ZA', 'TR', 'ID', 'TH', 'MY', 'PH', 'VN', 'PL', 'HU', 'RO',
                'BG', 'HR', 'AR', 'CL', 'CO', 'PE', 'UY', 'CR', 'PA', 'EG', 'MA', 'TN', 'JO', 'LB', 'AE',
                'SA', 'QA', 'KW', 'BH', 'OM'
            ) then 'EMERGING'
            else 'FRONTIER'
        end as economic_development_status,
        
        -- Private markets activity level
        case 
            when country_code in ('US', 'GB', 'DE', 'FR', 'CA', 'AU', 'JP', 'SG', 'HK') then 'HIGH_ACTIVITY'
            when country_code in ('IT', 'ES', 'NL', 'SE', 'CH', 'NO', 'DK', 'FI', 'KR', 'CN', 'IN', 'BR') then 'MEDIUM_ACTIVITY'
            when country_code in ('MX', 'ZA', 'TR', 'ID', 'TH', 'MY', 'PL', 'CZ', 'AR', 'CL', 'AE', 'SA') then 'LOW_ACTIVITY'
            else 'MINIMAL_ACTIVITY'
        end as private_markets_activity_level,
        
        -- Regulatory environment assessment
        case 
            when country_code in ('US', 'GB', 'DE', 'FR', 'CA', 'AU', 'SG', 'HK', 'CH', 'NL', 'SE', 'NO', 'DK') then 'FAVORABLE'
            when country_code in ('IT', 'ES', 'JP', 'KR', 'FI', 'AT', 'BE', 'IE', 'LU', 'NZ', 'IL') then 'MODERATE'
            when country_code in ('CN', 'IN', 'BR', 'MX', 'ZA', 'TR', 'PL', 'CZ', 'CL', 'AE') then 'RESTRICTIVE'
            else 'UNKNOWN'
        end as regulatory_environment,
        
        -- Tax efficiency for private markets
        case 
            when country_code in ('LU', 'IE', 'NL', 'CH', 'SG', 'HK', 'AE', 'BH', 'QA', 'KW') then 'HIGH_EFFICIENCY'
            when country_code in ('US', 'GB', 'DE', 'CA', 'AU', 'DK', 'SE', 'NO', 'FI') then 'MEDIUM_EFFICIENCY'
            when country_code in ('FR', 'IT', 'ES', 'JP', 'KR', 'BR', 'IN', 'ZA') then 'LOW_EFFICIENCY'
            else 'UNKNOWN_EFFICIENCY'
        end as tax_efficiency_rating,
        
        -- Currency stability assessment
        case 
            when primary_currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CHF') then 'HIGHLY_STABLE'
            when primary_currency_code in ('CAD', 'AUD', 'SEK', 'NOK', 'DKK', 'SGD', 'HKD') then 'STABLE'
            when primary_currency_code in ('CNY', 'KRW', 'INR', 'BRL', 'MXN', 'ZAR') then 'MODERATELY_STABLE'
            else 'VOLATILE'
        end as currency_stability,
        
        -- G7/G20 membership
        case 
            when country_code in ('US', 'CA', 'GB', 'DE', 'FR', 'IT', 'JP') then 'G7'
            when country_code in ('AR', 'AU', 'BR', 'CN', 'IN', 'ID', 'KR', 'MX', 'RU', 'SA', 'ZA', 'TR') then 'G20_ONLY'
            else 'NEITHER'
        end as g7_g20_membership

    from cleaned
),

final as (
    select
        *,
        
        -- Overall country data quality
        case 
            when is_valid_iso_alpha_2 = true 
                and is_valid_iso_alpha_3 = true
                and country_name is not null
                and standardized_region != 'OTHER'
                and primary_currency_code is not null
            then 'HIGH_QUALITY'
            when is_valid_iso_alpha_2 = true 
                and country_name is not null
                and standardized_region != 'OTHER'
            then 'MEDIUM_QUALITY'
            when country_code is not null
                and country_name is not null
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as data_quality_rating,
        
        -- Recommended for private markets investment
        case 
            when private_markets_activity_level in ('HIGH_ACTIVITY', 'MEDIUM_ACTIVITY')
                and regulatory_environment in ('FAVORABLE', 'MODERATE')
                and economic_development_status in ('DEVELOPED', 'EMERGING')
                and is_active = true
            then true
            else false
        end as recommended_for_investment,
        
        -- Completeness score
        (
            case when country_name is not null then 1 else 0 end +
            case when iso_alpha_3_code is not null then 1 else 0 end +
            case when iso_numeric_code is not null then 1 else 0 end +
            case when standardized_region != 'OTHER' then 1 else 0 end +
            case when standardized_sub_region != 'OTHER' then 1 else 0 end +
            case when capital_city is not null then 1 else 0 end +
            case when primary_currency_code is not null then 1 else 0 end +
            case when international_phone_code is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(country_code, country_name, iso_alpha_3_code, iso_numeric_code, region, sub_region, capital_city, primary_currency_code, is_active, last_modified_date)) as record_hash

    from enhanced
)

select * from final