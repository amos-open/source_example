{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging model for company cross-reference data
  
  This model cleans and standardizes company cross-reference mappings,
  enabling entity resolution across multiple source systems.
*/

with source as (
    select * from {{ source('reference', 'amos_xref_companies') }}
),

cleaned as (
    select
        trim(canonical_id) as canonical_company_id,
        trim(source_id) as crm_company_id,
        null as pm_company_id,
        null as accounting_entity_id,
        trim(company_name) as canonical_company_name,
        NULL as resolution_confidence,
        upper(trim(source_system)) as primary_source_system,
        
        case when created_date is not null then CAST(created_date AS DATE) else null end as created_date,
        case when last_modified_date is not null then CAST(last_modified_date AS DATE) else null end as last_modified_date,
        
        'REFERENCE' as source_system,
        'amos_xref_companies' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where canonical_id is not null
),

enhanced as (
    select
        *,
        
        case 
            when resolution_confidence in ('HIGH', 'STRONG') then 'HIGH'
            when resolution_confidence in ('MEDIUM', 'MODERATE') then 'MEDIUM'
            when resolution_confidence in ('LOW', 'WEAK') then 'LOW'
            else 'UNKNOWN'
        end as standardized_confidence,
        
        0 as source_systems_count,
        
        'UNKNOWN' as canonical_id_validation

    from cleaned
),

final as (
    select
        *,
        
        'LOW_QUALITY' as data_quality_rating,
        
        MD5(CONCAT(canonical_company_id, coalesce(crm_company_id,''), coalesce(pm_company_id,''), coalesce(accounting_entity_id,''), coalesce(canonical_company_name,''), coalesce(resolution_confidence,''), coalesce(to_char(last_modified_date),'') )) as record_hash

    from enhanced
)

select * from final