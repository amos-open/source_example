{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging model for reference industries data
  
  This model cleans and standardizes industry reference data,
  providing a master list of industry classifications used across the system.
  
  Transformations applied:
  - Validate industry codes and hierarchical structure
  - Standardize industry names and classifications
  - Handle GICS (Global Industry Classification Standard) mapping
  - Add industry usage and validation flags for private markets
*/

with source as (
    select * from {{ source('reference', 'amos_ref_industries') }}
),

cleaned as (
    select
        -- Primary identifier
        upper(trim(industry_code)) as industry_code,
        
        -- Industry details
        trim(industry_name) as industry_name,
        upper(trim(industry_classification)) as classification_system,
        upper(trim(parent_industry_code)) as parent_industry_code,
        
        case 
            when industry_level is not null 
                and industry_level between 1 and 5
            then cast(industry_level as number(1,0))
            else null
        end as industry_level,
        
        -- GICS classification hierarchy
        trim(gics_sector) as gics_sector,
        trim(gics_industry_group) as gics_industry_group,
        trim(gics_industry) as gics_industry,
        trim(gics_sub_industry) as gics_sub_industry,
        
        case 
            when upper(trim(is_active)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(is_active)) in ('NO', 'FALSE', '0', 'N') then false
            else true  -- Default to active
        end as is_active,
        
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
        'amos_ref_industries' as source_table,
        current_timestamp() as loaded_at

    from source
    where industry_code is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Industry hierarchy validation
        case 
            when industry_level = 1 and parent_industry_code is null then 'VALID_ROOT'
            when industry_level > 1 and parent_industry_code is not null then 'VALID_CHILD'
            when industry_level = 1 and parent_industry_code is not null then 'INVALID_ROOT_WITH_PARENT'
            when industry_level > 1 and parent_industry_code is null then 'INVALID_CHILD_WITHOUT_PARENT'
            else 'UNKNOWN_HIERARCHY'
        end as hierarchy_validation_status,
        
        -- GICS sector standardization
        case 
            when upper(gics_sector) like '%INFORMATION TECHNOLOGY%' then 'INFORMATION_TECHNOLOGY'
            when upper(gics_sector) like '%HEALTH CARE%' then 'HEALTH_CARE'
            when upper(gics_sector) like '%FINANCIALS%' then 'FINANCIALS'
            when upper(gics_sector) like '%CONSUMER DISCRETIONARY%' then 'CONSUMER_DISCRETIONARY'
            when upper(gics_sector) like '%CONSUMER STAPLES%' then 'CONSUMER_STAPLES'
            when upper(gics_sector) like '%INDUSTRIALS%' then 'INDUSTRIALS'
            when upper(gics_sector) like '%MATERIALS%' then 'MATERIALS'
            when upper(gics_sector) like '%ENERGY%' then 'ENERGY'
            when upper(gics_sector) like '%UTILITIES%' then 'UTILITIES'
            when upper(gics_sector) like '%REAL ESTATE%' then 'REAL_ESTATE'
            when upper(gics_sector) like '%COMMUNICATION%' then 'COMMUNICATION_SERVICES'
            else 'OTHER'
        end as standardized_gics_sector,
        
        -- Private markets sector mapping
        case 
            when upper(industry_name) like '%TECHNOLOGY%' 
                or upper(industry_name) like '%SOFTWARE%' 
                or upper(industry_name) like '%INTERNET%' 
                or upper(industry_name) like '%SEMICONDUCTOR%' then 'TECHNOLOGY'
            when upper(industry_name) like '%HEALTHCARE%' 
                or upper(industry_name) like '%PHARMACEUTICAL%' 
                or upper(industry_name) like '%BIOTECHNOLOGY%' 
                or upper(industry_name) like '%MEDICAL%' then 'HEALTHCARE'
            when upper(industry_name) like '%FINANCIAL%' 
                or upper(industry_name) like '%FINTECH%' 
                or upper(industry_name) like '%BANKING%' 
                or upper(industry_name) like '%INSURANCE%' then 'FINANCIAL_SERVICES'
            when upper(industry_name) like '%CONSUMER%' 
                or upper(industry_name) like '%RETAIL%' 
                or upper(industry_name) like '%RESTAURANT%' 
                or upper(industry_name) like '%APPAREL%' then 'CONSUMER'
            when upper(industry_name) like '%INDUSTRIAL%' 
                or upper(industry_name) like '%MANUFACTURING%' 
                or upper(industry_name) like '%AEROSPACE%' 
                or upper(industry_name) like '%AUTOMOTIVE%' then 'INDUSTRIALS'
            when upper(industry_name) like '%ENERGY%' 
                or upper(industry_name) like '%OIL%' 
                or upper(industry_name) like '%RENEWABLE%' 
                or upper(industry_name) like '%UTILITIES%' then 'ENERGY'
            when upper(industry_name) like '%REAL ESTATE%' 
                or upper(industry_name) like '%PROPERTY%' 
                or upper(industry_name) like '%REIT%' then 'REAL_ESTATE'
            when upper(industry_name) like '%MEDIA%' 
                or upper(industry_name) like '%ENTERTAINMENT%' 
                or upper(industry_name) like '%TELECOM%' then 'MEDIA_TELECOM'
            when upper(industry_name) like '%EDUCATION%' 
                or upper(industry_name) like '%EDTECH%' then 'EDUCATION'
            when upper(industry_name) like '%AGRICULTURE%' 
                or upper(industry_name) like '%FOOD%' then 'AGRICULTURE'
            else 'OTHER'
        end as private_markets_sector,
        
        -- Investment attractiveness for private markets
        case 
            when private_markets_sector in ('TECHNOLOGY', 'HEALTHCARE', 'FINANCIAL_SERVICES') then 'HIGH_INTEREST'
            when private_markets_sector in ('CONSUMER', 'INDUSTRIALS', 'MEDIA_TELECOM') then 'MEDIUM_INTEREST'
            when private_markets_sector in ('ENERGY', 'REAL_ESTATE', 'EDUCATION') then 'SELECTIVE_INTEREST'
            else 'LOW_INTEREST'
        end as pe_investment_attractiveness,
        
        -- Growth potential assessment
        case 
            when private_markets_sector in ('TECHNOLOGY', 'HEALTHCARE') 
                and upper(industry_name) like '%SOFTWARE%' 
                or upper(industry_name) like '%BIOTECH%' 
                or upper(industry_name) like '%FINTECH%' then 'HIGH_GROWTH'
            when private_markets_sector in ('TECHNOLOGY', 'HEALTHCARE', 'FINANCIAL_SERVICES', 'CONSUMER') then 'MEDIUM_GROWTH'
            when private_markets_sector in ('INDUSTRIALS', 'ENERGY', 'REAL_ESTATE') then 'STABLE_GROWTH'
            else 'LOW_GROWTH'
        end as growth_potential,
        
        -- Cyclicality assessment
        case 
            when private_markets_sector in ('TECHNOLOGY', 'HEALTHCARE') then 'LOW_CYCLICAL'
            when private_markets_sector in ('CONSUMER', 'FINANCIAL_SERVICES', 'MEDIA_TELECOM') then 'MEDIUM_CYCLICAL'
            when private_markets_sector in ('INDUSTRIALS', 'ENERGY', 'REAL_ESTATE') then 'HIGH_CYCLICAL'
            else 'UNKNOWN_CYCLICAL'
        end as cyclicality_assessment,
        
        -- ESG considerations
        case 
            when upper(industry_name) like '%RENEWABLE%' 
                or upper(industry_name) like '%CLEAN%' 
                or upper(industry_name) like '%SUSTAINABLE%' then 'ESG_POSITIVE'
            when upper(industry_name) like '%OIL%' 
                or upper(industry_name) like '%COAL%' 
                or upper(industry_name) like '%TOBACCO%' 
                or upper(industry_name) like '%WEAPONS%' then 'ESG_NEGATIVE'
            when private_markets_sector in ('HEALTHCARE', 'EDUCATION', 'TECHNOLOGY') then 'ESG_NEUTRAL_POSITIVE'
            else 'ESG_NEUTRAL'
        end as esg_classification,
        
        -- Regulatory intensity
        case 
            when private_markets_sector in ('HEALTHCARE', 'FINANCIAL_SERVICES', 'ENERGY') then 'HIGH_REGULATION'
            when private_markets_sector in ('TECHNOLOGY', 'MEDIA_TELECOM', 'EDUCATION') then 'MEDIUM_REGULATION'
            when private_markets_sector in ('CONSUMER', 'INDUSTRIALS', 'REAL_ESTATE') then 'LOW_REGULATION'
            else 'UNKNOWN_REGULATION'
        end as regulatory_intensity

    from cleaned
),

final as (
    select
        *,
        
        -- Overall industry data quality
        case 
            when hierarchy_validation_status like 'VALID%'
                and industry_name is not null
                and classification_system is not null
                and standardized_gics_sector != 'OTHER'
            then 'HIGH_QUALITY'
            when hierarchy_validation_status like 'VALID%'
                and industry_name is not null
                and classification_system is not null
            then 'MEDIUM_QUALITY'
            when industry_code is not null
                and industry_name is not null
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as data_quality_rating,
        
        -- Recommended for private markets tracking
        case 
            when pe_investment_attractiveness in ('HIGH_INTEREST', 'MEDIUM_INTEREST')
                and growth_potential in ('HIGH_GROWTH', 'MEDIUM_GROWTH')
                and esg_classification != 'ESG_NEGATIVE'
                and is_active = true
            then true
            else false
        end as recommended_for_tracking,
        
        -- Completeness score
        (
            case when industry_name is not null then 1 else 0 end +
            case when classification_system is not null then 1 else 0 end +
            case when industry_level is not null then 1 else 0 end +
            case when gics_sector is not null then 1 else 0 end +
            case when gics_industry_group is not null then 1 else 0 end +
            case when gics_industry is not null then 1 else 0 end +
            case when hierarchy_validation_status like 'VALID%' then 1 else 0 end
        ) / 7.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        hash(
            industry_code,
            industry_name,
            classification_system,
            parent_industry_code,
            industry_level,
            gics_sector,
            gics_industry_group,
            gics_industry,
            gics_sub_industry,
            is_active,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final