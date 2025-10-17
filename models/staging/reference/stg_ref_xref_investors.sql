{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging model for investor cross-reference data
*/

with source as (
    select * from {{ source('reference', 'amos_xref_investors') }}
),

cleaned as (
    select
        trim(canonical_investor_id) as canonical_investor_id,
        trim(admin_investor_code) as admin_investor_code,
        trim(crm_investor_id) as crm_investor_id,
        trim(investor_name) as canonical_investor_name,
        
        case when created_date is not null then cast(created_date as date) else null end as created_date,
        case when last_modified_date is not null then cast(last_modified_date as date) else null end as last_modified_date,
        
        'REFERENCE' as source_system,
        'amos_xref_investors' as source_table,
        current_timestamp() as loaded_at

    from source
    where canonical_investor_id is not null
),

final as (
    select
        *,
        
        (
            case when admin_investor_code is not null then 1 else 0 end +
            case when crm_investor_id is not null then 1 else 0 end
        ) as source_systems_count,
        
        case 
            when canonical_investor_id like 'INV-CANON-%' then 'VALID_FORMAT'
            else 'INVALID_FORMAT'
        end as canonical_id_validation,
        
        hash(
            canonical_investor_id,
            admin_investor_code,
            crm_investor_id,
            canonical_investor_name,
            last_modified_date
        ) as record_hash

    from cleaned
)

select * from final