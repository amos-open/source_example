{{
  config(
    materialized='view',
    tags=['crm', 'staging']
  )
}}

/*
  Staging model for CRM companies data
  
  This model cleans and standardizes company data from the CRM system,
  handling data quality issues and preparing data for entity resolution.
  
  Transformations applied:
  - Standardize company names and legal names
  - Normalize industry classifications
  - Validate and standardize country codes
  - Parse employee count ranges
  - Clean website URLs
  - Handle revenue range parsing
  - Add data quality scoring
*/

with source as (
    select * from {{ source('crm_vendor', 'amos_crm_companies') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(company_id) as company_id,
        
        -- Company names
        trim(company_name) as company_name,
        trim(legal_name) as legal_name,
        
        -- Industry classifications
        trim(industry_primary) as industry_primary,
        trim(industry_secondary) as industry_secondary,
        
        -- Geographic information
        upper(trim(country_code)) as country_code,
        trim(state_province) as state_province,
        trim(city) as city,
        
        -- Company details
        case 
            when founded_year is not null 
                and founded_year between 1800 and year(current_date())
            then cast(founded_year as number(4,0))
            else null
        end as founded_year,
        
        case 
            when employee_count is not null and employee_count > 0
            then cast(employee_count as number(10,0))
            else null
        end as employee_count,
        
        -- Revenue range parsing
        trim(revenue_range) as revenue_range_text,
        
        -- Website cleaning
        case 
            when website is not null and website != ''
            then lower(trim(
                case 
                    when website like 'http%' then website
                    else 'https://' || website
                end
            ))
            else null
        end as website_url,
        
        -- Business information
        trim(description) as company_description,
        trim(business_model) as business_model,
        trim(competitive_position) as competitive_position,
        trim(key_risks) as key_risks,
        
        -- ESG and data quality scores
        case 
            when esg_score is not null 
                and esg_score between 0 and 100
            then cast(esg_score as number(5,2))
            else null
        end as esg_score,
        
        case 
            when data_quality_score is not null 
                and data_quality_score between 0 and 100
            then cast(data_quality_score as number(5,2))
            else null
        end as data_quality_score,
        
        -- Currency and industry mapping
        upper(trim(base_currency_code)) as base_currency_code,
        trim(industry_id) as industry_id,
        
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
        'CRM_VENDOR' as source_system,
        'amos_crm_companies' as source_table,
        current_timestamp() as loaded_at

    from source
    where company_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Parse revenue range into numeric values (in millions)
        case 
            when revenue_range_text like '%-%M' then
                case 
                    when revenue_range_text like '%-5M' then 2.5
                    when revenue_range_text like '5-10M' then 7.5
                    when revenue_range_text like '10-25M' then 17.5
                    when revenue_range_text like '25-50M' then 37.5
                    when revenue_range_text like '50-100M' then 75
                    when revenue_range_text like '100-250M' then 175
                    when revenue_range_text like '250-500M' then 375
                    when revenue_range_text like '500M+' then 750
                    else null
                end
            else null
        end as revenue_midpoint_millions,
        
        -- Employee size category
        case 
            when employee_count is null then 'UNKNOWN'
            when employee_count < 50 then 'SMALL'
            when employee_count < 250 then 'MEDIUM'
            when employee_count < 1000 then 'LARGE'
            else 'ENTERPRISE'
        end as company_size_category,
        
        -- Standardize country codes to ISO 3166-1 alpha-2
        case 
            when upper(country_code) in ('US', 'USA', 'UNITED STATES') then 'US'
            when upper(country_code) in ('UK', 'GB', 'UNITED KINGDOM') then 'GB'
            when upper(country_code) in ('DE', 'GERMANY') then 'DE'
            when upper(country_code) in ('FR', 'FRANCE') then 'FR'
            when upper(country_code) in ('JP', 'JAPAN') then 'JP'
            when upper(country_code) in ('SG', 'SINGAPORE') then 'SG'
            when upper(country_code) in ('CA', 'CANADA') then 'CA'
            when upper(country_code) in ('AU', 'AUSTRALIA') then 'AU'
            else upper(country_code)
        end as standardized_country_code,
        
        -- Industry sector mapping
        case 
            when lower(industry_primary) like '%technology%' 
                or lower(industry_primary) like '%software%' 
                or lower(industry_primary) like '%tech%' then 'TECHNOLOGY'
            when lower(industry_primary) like '%healthcare%' 
                or lower(industry_primary) like '%medical%' 
                or lower(industry_primary) like '%pharma%' then 'HEALTHCARE'
            when lower(industry_primary) like '%financial%' 
                or lower(industry_primary) like '%fintech%' 
                or lower(industry_primary) like '%banking%' then 'FINANCIAL_SERVICES'
            when lower(industry_primary) like '%energy%' 
                or lower(industry_primary) like '%renewable%' 
                or lower(industry_primary) like '%oil%' then 'ENERGY'
            when lower(industry_primary) like '%consumer%' 
                or lower(industry_primary) like '%retail%' then 'CONSUMER'
            when lower(industry_primary) like '%industrial%' 
                or lower(industry_primary) like '%manufacturing%' 
                or lower(industry_primary) like '%aerospace%' then 'INDUSTRIALS'
            when lower(industry_primary) like '%real estate%' 
                or lower(industry_primary) like '%property%' then 'REAL_ESTATE'
            when lower(industry_primary) like '%education%' 
                or lower(industry_primary) like '%edtech%' then 'EDUCATION'
            when lower(industry_primary) like '%agriculture%' 
                or lower(industry_primary) like '%agtech%' then 'AGRICULTURE'
            when lower(industry_primary) like '%transportation%' 
                or lower(industry_primary) like '%logistics%' then 'TRANSPORTATION'
            else 'OTHER'
        end as industry_sector,
        
        -- Data completeness score
        (
            case when company_name is not null then 1 else 0 end +
            case when industry_primary is not null then 1 else 0 end +
            case when standardized_country_code is not null then 1 else 0 end +
            case when founded_year is not null then 1 else 0 end +
            case when employee_count is not null then 1 else 0 end +
            case when revenue_range_text is not null then 1 else 0 end +
            case when website_url is not null then 1 else 0 end +
            case when company_description is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score

    from cleaned
),

final as (
    select
        *,
        
        -- Overall data quality assessment
        case 
            when completeness_score >= 80 and data_quality_score >= 85 then 'HIGH'
            when completeness_score >= 60 and data_quality_score >= 70 then 'MEDIUM'
            else 'LOW'
        end as overall_data_quality,
        
        -- Record hash for change detection
        hash(
            company_id,
            company_name,
            legal_name,
            industry_primary,
            country_code,
            founded_year,
            employee_count,
            revenue_range_text,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final