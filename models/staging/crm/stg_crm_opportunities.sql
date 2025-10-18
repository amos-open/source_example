{{
  config(
    materialized='view',
    tags=['crm', 'staging']
  )
}}

/*
  Staging model for CRM opportunities data
  
  This model cleans and standardizes opportunity data from the CRM system,
  handling data quality issues and preparing data for downstream processing.
  
  Transformations applied:
  - Standardize text fields (trim, upper case for codes)
  - Parse and validate dates
  - Handle null values appropriately
  - Cast numeric fields to proper precision
  - Add source system metadata
  - Standardize currency codes
  - Clean and validate probability percentages
*/

with source as (
    select * from {{ source('crm_vendor', 'amos_crm_opportunities') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(opportunity_id) as opportunity_id,
        trim(company_id) as company_id,
        
        -- Opportunity details
        trim(opportunity_name) as opportunity_name,
        trim(company_name) as company_name,
        upper(trim(stage)) as stage,
        upper(trim(deal_type)) as deal_type,
        upper(trim(source)) as opportunity_source,
        
        -- Financial information
        case 
            when amount is not null and amount > 0 
            then CAST(amount AS NUMERIC(20,2))
            else null
        end as expected_amount,
        upper(trim(currency_code)) as currency_code,
        
        -- Probability handling - convert to decimal if percentage
        case 
            when probability is not null then
                case 
                    when probability > 1 then probability / 100.0
                    else probability
                end
            else null
        end as probability_decimal,
        
        -- Dates
        case 
            when expected_close_date is not null 
            then CAST(expected_close_date AS DATE)
            else null
        end as expected_close_date,
        
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
        
        -- Contact and ownership
        trim(owner_name) as deal_owner_name,
        
        -- Geographic and industry information
        trim(industry) as industry_name,
        trim(geography) as geography_region,
        
        -- Investment thesis and description
        trim(investment_thesis) as investment_thesis,
        trim(description) as opportunity_description,
        
        -- Data quality indicators
        case 
            when opportunity_id is not null 
                and opportunity_name is not null 
                and company_id is not null
                and stage is not null
            then 'COMPLETE'
            when opportunity_id is not null 
                and opportunity_name is not null
            then 'PARTIAL'
            else 'INCOMPLETE'
        end as data_quality_status,
        
        -- Source system metadata
        'CRM_VENDOR' as source_system,
        'amos_crm_opportunities' as source_table,
        CURRENT_TIMESTAMP() as loaded_at,
        
        -- Record hash for change detection
        TO_VARCHAR(MD5(CONCAT(
            COALESCE(opportunity_id, ''),
            COALESCE(opportunity_name, ''),
            COALESCE(company_id, ''),
            COALESCE(stage, ''),
            COALESCE(TO_VARCHAR(expected_close_date), ''),
            COALESCE(TO_VARCHAR(amount), ''),
            COALESCE(currency_code, ''),
            COALESCE(TO_VARCHAR(probability), ''),
            COALESCE(TO_VARCHAR(last_modified_date), '')
        ))) as record_hash

    from source
    where opportunity_id is not null  -- Filter out records without primary key
),

final as (
    select
        -- Generated ID for compatibility
        TO_VARCHAR(MD5(COALESCE(opportunity_id, ''))) as id,
        *,
        
        -- Add derived fields
        case 
            when stage in ('COMMITTED', 'LOI SIGNED', 'CLOSED WON') then 'ACTIVE'
            when stage in ('DECLINED', 'CLOSED LOST', 'ON HOLD') then 'INACTIVE'
            else 'PIPELINE'
        end as opportunity_status,
        
        -- Calculate days to expected close
        case 
            when expected_close_date is not null 
            then DATEDIFF('day', current_date(), expected_close_date)
            else null
        end as days_to_close,
        
        -- Validate currency codes (basic validation)
        case 
            when currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then currency_code
            else 'USD'  -- Default to USD for invalid codes
        end as validated_currency_code

    from cleaned
)

select * from final