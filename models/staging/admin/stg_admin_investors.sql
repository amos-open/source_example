{{
  config(
    materialized='view',
    tags=['fund_admin', 'staging']
  )
}}

/*
  Staging model for fund administration investors data
  
  This model cleans and standardizes investor master data from the fund administration system,
  handling investor classifications, compliance status, and contact information.
  
  Transformations applied:
  - Standardize investor names and legal names
  - Validate investor types and classifications
  - Standardize geographic information
  - Handle compliance and regulatory status
  - Clean contact information
  - Add investor categorization and scoring
*/

with source as (
    select * from {{ source('fund_admin_vendor', 'amos_admin_investors') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(investor_code) as investor_code,
        
        -- Investor names
        trim(investor_name) as investor_name,
        trim(investor_legal_name) as investor_legal_name,
        
        -- Investor classification
        trim(investor_type) as investor_type,
        trim(investor_type_id) as investor_type_id,
        
        -- Geographic information
        upper(trim(country_code)) as country_code,
        trim(state_province) as state_province,
        trim(city) as city,
        
        -- Contact information
        trim(contact_person) as contact_person_name,
        case 
            when contact_email is not null and contact_email like '%@%'
            then lower(trim(contact_email))
            else null
        end as contact_email,
        
        case 
            when contact_phone is not null and contact_phone != ''
            then REGEXP_REPLACE(trim(contact_phone), '[^0-9+()-]', '')
            else null
        end as contact_phone,
        
        -- Compliance and regulatory status
        upper(trim(kyc_status)) as kyc_status,
        upper(trim(aml_status)) as aml_status,
        upper(trim(accredited_status)) as accredited_status,
        
        -- Tax information
        trim(tax_id) as tax_identification_number,
        upper(trim(tax_jurisdiction)) as tax_jurisdiction,
        
        -- Investment characteristics
        upper(trim(investment_capacity)) as investment_capacity,
        upper(trim(risk_tolerance)) as risk_tolerance,
        upper(trim(liquidity_preference)) as liquidity_preference,
        
        case 
            when upper(trim(esg_requirements)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(esg_requirements)) in ('NO', 'FALSE', '0', 'N') then false
            else null
        end as has_esg_requirements,
        
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
        'amos_admin_investors' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where investor_code is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
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
            when upper(country_code) in ('CH', 'SWITZERLAND') then 'CH'
            when upper(country_code) in ('NL', 'NETHERLANDS') then 'NL'
            else upper(country_code)
        end as standardized_country_code,
        
        -- Investor type standardization and categorization
        case 
            when upper(investor_type) like '%PENSION%' then 'PENSION_FUND'
            when upper(investor_type) like '%ENDOWMENT%' then 'ENDOWMENT'
            when upper(investor_type) like '%FOUNDATION%' then 'FOUNDATION'
            when upper(investor_type) like '%INSURANCE%' then 'INSURANCE_COMPANY'
            when upper(investor_type) like '%SOVEREIGN%' then 'SOVEREIGN_WEALTH_FUND'
            when upper(investor_type) like '%FAMILY%' then 'FAMILY_OFFICE'
            when upper(investor_type) like '%FUND OF FUNDS%' then 'FUND_OF_FUNDS'
            when upper(investor_type) like '%BANK%' then 'BANK'
            when upper(investor_type) like '%CORPORATE%' then 'CORPORATE'
            when upper(investor_type) like '%INDIVIDUAL%' or upper(investor_type) like '%HNW%' then 'HIGH_NET_WORTH'
            else 'OTHER'
        end as standardized_investor_type,
        
        -- Investor size categorization based on type
        case 
            when standardized_investor_type in ('PENSION_FUND', 'SOVEREIGN_WEALTH_FUND', 'INSURANCE_COMPANY') 
                and investment_capacity = 'HIGH' then 'LARGE_INSTITUTIONAL'
            when standardized_investor_type in ('ENDOWMENT', 'FOUNDATION', 'FUND_OF_FUNDS')
                and investment_capacity in ('HIGH', 'MEDIUM') then 'MEDIUM_INSTITUTIONAL'
            when standardized_investor_type in ('FAMILY_OFFICE', 'HIGH_NET_WORTH') then 'PRIVATE_WEALTH'
            when standardized_investor_type in ('BANK', 'CORPORATE') then 'CORPORATE'
            else 'OTHER'
        end as investor_size_category,
        
        -- Compliance status assessment
        case 
            when kyc_status = 'APPROVED' 
                and aml_status = 'CLEARED' 
                and accredited_status in ('QUALIFIED', 'ACCREDITED') 
            then 'FULLY_COMPLIANT'
            when kyc_status in ('APPROVED', 'PENDING') 
                and aml_status in ('CLEARED', 'PENDING')
            then 'PARTIALLY_COMPLIANT'
            else 'NON_COMPLIANT'
        end as compliance_status,
        
        -- Investment profile scoring
        case 
            when investment_capacity = 'HIGH' then 3
            when investment_capacity = 'MEDIUM' then 2
            when investment_capacity = 'LOW' then 1
            else 0
        end as capacity_score,
        
        case 
            when risk_tolerance = 'HIGH' then 3
            when risk_tolerance = 'MEDIUM' then 2
            when risk_tolerance = 'LOW' then 1
            else 0
        end as risk_score,
        
        case 
            when liquidity_preference = 'LOW' then 3  -- Low liquidity preference is good for PE
            when liquidity_preference = 'MEDIUM' then 2
            when liquidity_preference = 'HIGH' then 1
            else 0
        end as liquidity_score,
        
        -- Regional classification
        case 
            when standardized_country_code in ('US', 'CA') then 'NORTH_AMERICA'
            when standardized_country_code in ('GB', 'DE', 'FR', 'CH', 'NL', 'IT', 'ES') then 'EUROPE'
            when standardized_country_code in ('JP', 'SG', 'HK', 'AU', 'KR') then 'ASIA_PACIFIC'
            when standardized_country_code in ('AE', 'SA', 'QA') then 'MIDDLE_EAST'
            else 'OTHER'
        end as geographic_region,
        
        -- Data completeness assessment
        (
            case when investor_name is not null then 1 else 0 end +
            case when investor_type is not null then 1 else 0 end +
            case when standardized_country_code is not null then 1 else 0 end +
            case when contact_person_name is not null then 1 else 0 end +
            case when contact_email is not null then 1 else 0 end +
            case when kyc_status is not null then 1 else 0 end +
            case when investment_capacity is not null then 1 else 0 end +
            case when risk_tolerance is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score

    from cleaned
),

final as (
    select
        -- Generate deterministic hash ID from investor_code for compatibility
        TO_VARCHAR(MD5(COALESCE(investor_code,''))) as id,
        *,
        
        -- Overall investor attractiveness score
        (capacity_score * 0.4 + risk_score * 0.3 + liquidity_score * 0.3) as investor_attractiveness_score,
        
        -- Investor status for fundraising
        case 
            when compliance_status = 'FULLY_COMPLIANT' 
                and investor_attractiveness_score >= 2.5 
            then 'TARGET'
            when compliance_status in ('FULLY_COMPLIANT', 'PARTIALLY_COMPLIANT') 
                and investor_attractiveness_score >= 2.0 
            then 'QUALIFIED'
            when compliance_status != 'NON_COMPLIANT' 
            then 'POTENTIAL'
            else 'EXCLUDED'
        end as fundraising_status,
        
        -- Overall data quality rating
        case 
            when completeness_score >= 90 and compliance_status = 'FULLY_COMPLIANT' then 'HIGH'
            when completeness_score >= 70 and compliance_status != 'NON_COMPLIANT' then 'MEDIUM'
            else 'LOW'
        end as data_quality_rating,
        
        -- Record hash for change detection
        TO_VARCHAR(MD5(CONCAT(
          COALESCE(investor_code,''),
          COALESCE(investor_name,''),
          COALESCE(investor_type,''),
          COALESCE(standardized_country_code,''),
          COALESCE(kyc_status,''),
          COALESCE(aml_status,''),
          COALESCE(accredited_status,''),
          COALESCE(investment_capacity,''),
          COALESCE(risk_tolerance,''),
          COALESCE(liquidity_preference,''),
          COALESCE(TO_VARCHAR(last_modified_date),'')
        ))) as record_hash

    from enhanced
)

select * from final