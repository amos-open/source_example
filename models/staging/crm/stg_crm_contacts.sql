{{
  config(
    materialized='view',
    tags=['crm', 'staging']
  )
}}

/*
  Staging model for CRM contacts data
  
  This model cleans and standardizes contact data from the CRM system,
  handling PII appropriately and preparing data for relationship analysis.
  
  Transformations applied:
  - Standardize name fields
  - Clean and validate email addresses
  - Format phone numbers consistently
  - Standardize role types and relationship data
  - Handle LinkedIn URL formatting
  - Add contact scoring and categorization
*/

with source as (
    select * from {{ source('crm_vendor', 'amos_crm_contacts') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(contact_id) as contact_id,
        trim(company_id) as company_id,
        
        -- Personal information
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        trim(title) as job_title,
        
        -- Contact information
        case 
            when email is not null and email like '%@%'
            then lower(trim(email))
            else null
        end as email_address,
        
        -- Phone number cleaning (basic formatting)
        case 
            when phone is not null and phone != ''
            then REGEXP_REPLACE(trim(phone), '[^0-9+()-]', '')
            else null
        end as phone_number,
        
        -- LinkedIn URL standardization
        case 
            when linkedin_url is not null and linkedin_url != ''
            then case 
                when linkedin_url like 'linkedin.com%' 
                then 'https://' || trim(linkedin_url)
                when linkedin_url like 'www.linkedin.com%'
                then 'https://' || trim(linkedin_url)
                when linkedin_url like 'http%'
                then trim(linkedin_url)
                else 'https://linkedin.com/in/' || trim(linkedin_url)
            end
            else null
        end as linkedin_profile_url,
        
        -- Role and relationship information
        upper(trim(role_type)) as role_type,
        
        case 
            when upper(trim(decision_maker)) in ('YES', 'TRUE', '1', 'Y') then true
            when upper(trim(decision_maker)) in ('NO', 'FALSE', '0', 'N') then false
            else null
        end as is_decision_maker,
        
        upper(trim(relationship_strength)) as relationship_strength,
        
        -- Contact activity
        case 
            when last_contact_date is not null 
            then CAST(last_contact_date AS DATE)
            else null
        end as last_contact_date,
        
        trim(contact_method) as last_contact_method,
        trim(notes) as contact_notes,
        
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
        'CRM_VENDOR' as source_system,
        'amos_crm_contacts' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where contact_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Full name construction
        case 
            when first_name is not null and last_name is not null
            then trim(first_name || ' ' || last_name)
            when first_name is not null
            then first_name
            when last_name is not null
            then last_name
            else null
        end as full_name,
        
        -- Role categorization
        case 
            when upper(role_type) in ('EXECUTIVE', 'CEO', 'PRESIDENT', 'FOUNDER') 
                or upper(job_title) like '%CEO%' 
                or upper(job_title) like '%PRESIDENT%'
                or upper(job_title) like '%FOUNDER%'
            then 'EXECUTIVE'
            when upper(role_type) in ('FINANCE', 'CFO') 
                or upper(job_title) like '%CFO%' 
                or upper(job_title) like '%FINANCE%'
            then 'FINANCE'
            when upper(role_type) in ('OPERATIONS', 'COO') 
                or upper(job_title) like '%COO%' 
                or upper(job_title) like '%OPERATIONS%'
            then 'OPERATIONS'
            when upper(job_title) like '%CTO%' 
                or upper(job_title) like '%TECHNOLOGY%'
                or upper(job_title) like '%TECHNICAL%'
            then 'TECHNOLOGY'
            else coalesce(role_type, 'OTHER')
        end as standardized_role_category,
        
        -- Seniority level assessment
        case 
            when upper(job_title) like '%CEO%' 
                or upper(job_title) like '%PRESIDENT%'
                or upper(job_title) like '%FOUNDER%'
                or upper(job_title) like '%MANAGING DIRECTOR%'
            then 'C_LEVEL'
            when upper(job_title) like '%CFO%' 
                or upper(job_title) like '%COO%'
                or upper(job_title) like '%CTO%'
                or upper(job_title) like '%VP%'
                or upper(job_title) like '%VICE PRESIDENT%'
            then 'VP_LEVEL'
            when upper(job_title) like '%DIRECTOR%'
                or upper(job_title) like '%HEAD OF%'
            then 'DIRECTOR_LEVEL'
            when upper(job_title) like '%MANAGER%'
                or upper(job_title) like '%LEAD%'
            then 'MANAGER_LEVEL'
            else 'OTHER'
        end as seniority_level,
        
        -- Contact recency scoring
        case 
            when last_contact_date is null then 0
            when DATEDIFF('day', last_contact_date, CURRENT_DATE()) <= 30 then 5
            when DATEDIFF('day', last_contact_date, CURRENT_DATE()) <= 90 then 4
            when DATEDIFF('day', last_contact_date, CURRENT_DATE()) <= 180 then 3
            when DATEDIFF('day', last_contact_date, CURRENT_DATE()) <= 365 then 2
            else 1
        end as contact_recency_score,
        
        -- Relationship strength scoring
        case 
            when relationship_strength = 'STRONG' then 3
            when relationship_strength = 'MEDIUM' then 2
            when relationship_strength = 'WEAK' then 1
            else 0
        end as relationship_score,
        
        -- Decision maker influence scoring
        case 
            when is_decision_maker = true and seniority_level = 'C_LEVEL' then 5
            when is_decision_maker = true and seniority_level = 'VP_LEVEL' then 4
            when is_decision_maker = true and seniority_level = 'DIRECTOR_LEVEL' then 3
            when is_decision_maker = true then 2
            when seniority_level in ('C_LEVEL', 'VP_LEVEL') then 2
            else 1
        end as influence_score,
        
        -- Data completeness assessment
        (
            case when full_name is not null then 1 else 0 end +
            case when job_title is not null then 1 else 0 end +
            case when email_address is not null then 1 else 0 end +
            case when phone_number is not null then 1 else 0 end +
            case when role_type is not null then 1 else 0 end +
            case when is_decision_maker is not null then 1 else 0 end +
            case when relationship_strength is not null then 1 else 0 end +
            case when last_contact_date is not null then 1 else 0 end
        ) / 8.0 * 100 as contact_completeness_score

    from cleaned
),

final as (
    select
        *,
        
        -- Overall contact value score (combination of factors)
        (
            contact_recency_score * 0.3 +
            relationship_score * 0.3 +
            influence_score * 0.4
        ) as contact_value_score,
        
        -- Contact status assessment
        case 
            when contact_recency_score >= 4 and relationship_score >= 2 then 'ACTIVE'
            when contact_recency_score >= 2 or relationship_score >= 2 then 'WARM'
            when last_contact_date is not null then 'COLD'
            else 'UNCONTACTED'
        end as contact_status,
        
        -- Record hash for change detection
        TO_VARCHAR(MD5(CONCAT(
          COALESCE(contact_id,''),
          COALESCE(company_id,''),
          COALESCE(first_name,''),
          COALESCE(last_name,''),
          COALESCE(job_title,''),
          COALESCE(email_address,''),
          COALESCE(role_type,''),
          COALESCE(TO_VARCHAR(is_decision_maker),''),
          COALESCE(relationship_strength,''),
          COALESCE(TO_VARCHAR(last_contact_date),''),
          COALESCE(TO_VARCHAR(last_modified_date),'')
        ))) as record_hash

    from enhanced
)

select * from final