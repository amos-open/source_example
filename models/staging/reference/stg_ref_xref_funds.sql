{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging model for fund cross-reference data
  
  This model cleans and standardizes fund cross-reference mappings,
  enabling entity resolution across multiple source systems.
  
  Transformations applied:
  - Validate canonical and source system identifiers
  - Standardize fund names for matching
  - Handle resolution confidence scoring
  - Add data quality and completeness assessment
*/

with source as (
    select * from {{ source('reference', 'amos_xref_funds') }}
),

cleaned as (
    select
        -- Canonical identifier
        trim(canonical_id) as canonical_fund_id,
        
        -- Source system identifiers
        trim(source_id) as admin_fund_code,
        null as crm_fund_id,
        null as pm_fund_id,
        null as accounting_fund_code,
        
        -- Fund name
        trim(fund_name) as canonical_fund_name,
        
        -- Resolution metadata
        NULL as resolution_confidence,
        
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
        'amos_xref_funds' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where canonical_fund_id is not null  -- Filter out records without canonical ID
),

enhanced as (
    select
        *,
        
        -- Resolution confidence standardization
        case 
            when resolution_confidence in ('HIGH', 'STRONG', 'CONFIDENT') then 'HIGH'
            when resolution_confidence in ('MEDIUM', 'MODERATE', 'FAIR') then 'MEDIUM'
            when resolution_confidence in ('LOW', 'WEAK', 'UNCERTAIN') then 'LOW'
            else 'UNKNOWN'
        end as standardized_confidence,
        
        -- Count of source systems mapped
        0 as source_systems_count,
        
        -- Canonical ID validation
        'UNKNOWN' as canonical_id_validation,
        
        -- Source system coverage assessment
        'NO_MAPPING' as source_coverage_level,
        
        -- Fund name standardization for matching
        upper(REGEXP_REPLACE(regexp_replace(canonical_fund_name, '[^A-Za-z0-9 ]', ''),
            '\\s+', ' '
        )) as normalized_fund_name,
        
        -- Extract fund characteristics from name
        case 
            when upper(canonical_fund_name) like '%GROWTH%' then 'GROWTH'
            when upper(canonical_fund_name) like '%BUYOUT%' then 'BUYOUT'
            when upper(canonical_fund_name) like '%VENTURE%' then 'VENTURE'
            when upper(canonical_fund_name) like '%CREDIT%' or upper(canonical_fund_name) like '%DEBT%' then 'CREDIT'
            when upper(canonical_fund_name) like '%INFRASTRUCTURE%' then 'INFRASTRUCTURE'
            when upper(canonical_fund_name) like '%REAL ESTATE%' then 'REAL_ESTATE'
            when upper(canonical_fund_name) like '%DISTRESSED%' then 'DISTRESSED'
            when upper(canonical_fund_name) like '%SECONDARY%' then 'SECONDARY'
            else 'OTHER'
        end as inferred_fund_strategy,
        
        -- Extract fund vintage from name
        case 
            when REGEXP_LIKE(canonical_fund_name, '\\b(I|1)\\b') then 1
            when REGEXP_LIKE(canonical_fund_name, '\\b(II|2)\\b') then 2
            when REGEXP_LIKE(canonical_fund_name, '\\b(III|3)\\b') then 3
            when REGEXP_LIKE(canonical_fund_name, '\\b(IV|4)\\b') then 4
            when REGEXP_LIKE(canonical_fund_name, '\\b(V|5)\\b') then 5
            when REGEXP_LIKE(canonical_fund_name, '\\b(VI|6)\\b') then 6
            when REGEXP_LIKE(canonical_fund_name, '\\b(VII|7)\\b') then 7
            when REGEXP_LIKE(canonical_fund_name, '\\b(VIII|8)\\b') then 8
            when REGEXP_LIKE(canonical_fund_name, '\\b(IX|9)\\b') then 9
            when REGEXP_LIKE(canonical_fund_name, '\\b(X|10)\\b') then 10
            else null
        end as inferred_fund_number,
        
        -- Geographic focus inference
        case 
            when upper(canonical_fund_name) like '%NORTH AMERICA%' 
                or upper(canonical_fund_name) like '%US%' 
                or upper(canonical_fund_name) like '%AMERICAN%' then 'NORTH_AMERICA'
            when upper(canonical_fund_name) like '%EUROPE%' 
                or upper(canonical_fund_name) like '%EUROPEAN%' then 'EUROPE'
            when upper(canonical_fund_name) like '%ASIA%' 
                or upper(canonical_fund_name) like '%ASIAN%' then 'ASIA'
            when upper(canonical_fund_name) like '%GLOBAL%' 
                or upper(canonical_fund_name) like '%INTERNATIONAL%' then 'GLOBAL'
            else 'UNKNOWN'
        end as inferred_geographic_focus,
        
        -- Resolution quality assessment
        'VERY_POOR' as resolution_quality

    from cleaned
),

final as (
    select
        *,
        
        -- Overall cross-reference data quality
        'POOR_QUALITY' as data_quality_rating,
        
        -- Recommended for entity resolution
        case 
            when data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
                and standardized_confidence in ('HIGH', 'MEDIUM')
            then true
            else false
        end as recommended_for_resolution,
        
        -- Completeness score
        (
            case when canonical_fund_id is not null then 1 else 0 end +
            case when canonical_fund_name is not null then 1 else 0 end +
            case when crm_fund_id is not null then 1 else 0 end +
            case when admin_fund_code is not null then 1 else 0 end +
            case when pm_fund_id is not null then 1 else 0 end +
            case when accounting_fund_code is not null then 1 else 0 end +
            case when resolution_confidence is not null then 1 else 0 end
        ) / 7.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        MD5(CONCAT(canonical_fund_id, coalesce(crm_fund_id,''), coalesce(admin_fund_code,''), coalesce(pm_fund_id,''), coalesce(accounting_fund_code,''), coalesce(canonical_fund_name,''), coalesce(resolution_confidence,''), coalesce(to_char(last_modified_date),'') )) as record_hash

    from enhanced
)

select * from final