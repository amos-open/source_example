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
        trim(canonical_company_id) as canonical_company_id,
        trim(crm_company_id) as crm_company_id,
        trim(pm_company_id) as pm_company_id,
        trim(accounting_entity_id) as accounting_entity_id,
        trim(company_name_canonical) as canonical_company_name,
        upper(trim(resolution_confidence)) as resolution_confidence,
        upper(trim(primary_source_system)) as primary_source_system,
        
        case when created_date is not null then cast(created_date as date) else null end as created_date,
        case when last_modified_date is not null then cast(last_modified_date as date) else null end as last_modified_date,
        
        'REFERENCE' as source_system,
        'amos_xref_companies' as source_table,
        current_timestamp() as loaded_at

    from source
    where canonical_company_id is not null
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
        
        (
            case when crm_company_id is not null then 1 else 0 end +
            case when pm_company_id is not null then 1 else 0 end +
            case when accounting_entity_id is not null then 1 else 0 end
        ) as source_systems_count,
        
        case 
            when canonical_company_id like 'COMP-CANON-%' then 'VALID_FORMAT'
            else 'INVALID_FORMAT'
        end as canonical_id_validation

    from cleaned
),

final as (
    select
        *,
        
        case 
            when canonical_id_validation = 'VALID_FORMAT'
                and standardized_confidence = 'HIGH'
                and source_systems_count >= 2
            then 'HIGH_QUALITY'
            when canonical_id_validation = 'VALID_FORMAT'
                and source_systems_count >= 1
            then 'MEDIUM_QUALITY'
            else 'LOW_QUALITY'
        end as data_quality_rating,
        
        hash(
            canonical_company_id,
            crm_company_id,
            pm_company_id,
            accounting_entity_id,
            canonical_company_name,
            resolution_confidence,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final