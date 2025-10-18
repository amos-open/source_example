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
        trim(canonical_fund_id) as canonical_fund_id,
        
        -- Source system identifiers
        trim(crm_fund_id) as crm_fund_id,
        trim(admin_fund_code) as admin_fund_code,
        trim(pm_fund_id) as pm_fund_id,
        trim(accounting_fund_code) as accounting_fund_code,
        
        -- Fund name
        trim(fund_name_canonical) as canonical_fund_name,
        
        -- Resolution metadata
        upper(trim(resolution_confidence)) as resolution_confidence,
        
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
        (
            case when crm_fund_id is not null then 1 else 0 end +
            case when admin_fund_code is not null then 1 else 0 end +
            case when pm_fund_id is not null then 1 else 0 end +
            case when accounting_fund_code is not null then 1 else 0 end
        ) as source_systems_count,
        
        -- Canonical ID validation
        case 
            when canonical_fund_id like 'FUND-CANON-%' then 'VALID_FORMAT'
            when canonical_fund_id is not null then 'INVALID_FORMAT'
            else 'MISSING'
        end as canonical_id_validation,
        
        -- Source system coverage assessment
        case 
            when source_systems_count >= 3 then 'COMPREHENSIVE'
            when source_systems_count = 2 then 'PARTIAL'
            when source_systems_count = 1 then 'MINIMAL'
            else 'NO_MAPPING'
        end as source_coverage_level,
        
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
        case 
            when standardized_confidence = 'HIGH' and source_systems_count >= 3 then 'EXCELLENT'
            when standardized_confidence = 'HIGH' and source_systems_count >= 2 then 'GOOD'
            when standardized_confidence = 'MEDIUM' and source_systems_count >= 2 then 'FAIR'
            when standardized_confidence = 'LOW' or source_systems_count = 1 then 'POOR'
            else 'VERY_POOR'
        end as resolution_quality

    from cleaned
),

final as (
    select
        *,
        
        -- Overall cross-reference data quality
        case 
            when canonical_id_validation = 'VALID_FORMAT'
                and resolution_quality in ('EXCELLENT', 'GOOD')
                and canonical_fund_name is not null
            then 'HIGH_QUALITY'
            when canonical_id_validation = 'VALID_FORMAT'
                and resolution_quality in ('EXCELLENT', 'GOOD', 'FAIR')
            then 'MEDIUM_QUALITY'
            when canonical_fund_id is not null
                and source_systems_count > 0
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as data_quality_rating,
        
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
        FARM_FINGERPRINT(CONCAT(canonical_fund_id, crm_fund_id, admin_fund_code, pm_fund_id, accounting_fund_code, canonical_fund_name, resolution_confidence, last_modified_date)) as record_hash

    from enhanced
)

select * from final