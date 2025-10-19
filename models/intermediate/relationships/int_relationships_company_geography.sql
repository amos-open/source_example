{{
  config(
    materialized='table',
    tags=['intermediate', 'relationships']
  )
}}

/*
  Intermediate model for company-geography relationship preparation
  
  This model prepares company-geography relationships from multiple data sources,
  creating bridge table records that capture geographic exposure and allocation
  for portfolio companies across different geographic regions.
  
  Data sources:
  - CRM companies (primary geographic data)
  - Portfolio management investments (geographic focus)
  - Company cross-reference mappings for entity resolution
  - Reference geography data for standardization
  
  Business logic:
  - Create company-geography relationships with allocation percentages
  - Handle primary vs secondary geographic exposures
  - Standardize geographic classifications and regions
  - Prepare geographic relationship data for canonical bridge tables
*/

with crm_companies as (
    select * from {{ ref('stg_crm_companies') }}
),

pm_investments as (
    select * from {{ ref('stg_pm_investments') }}
),

company_xref as (
    select * from {{ ref('stg_ref_xref_companies') }}
    where data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

ref_countries as (
    select * from {{ ref('stg_ref_countries') }}
),

-- Extract geographic information from CRM companies
crm_company_geography as (
    select
        cc.company_id as source_company_id,
        cc.company_name,
        cc.standardized_country_code as primary_country_code,
        cc.state_province,
        cc.city,
        
        -- Infer geographic region from country
        case 
            when cc.standardized_country_code in ('US', 'CA', 'MX') then 'NORTH_AMERICA'
            when cc.standardized_country_code in ('GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'CH', 'SE', 'NO', 'DK') then 'EUROPE'
            when cc.standardized_country_code in ('JP', 'CN', 'KR', 'SG', 'HK', 'AU', 'IN') then 'ASIA_PACIFIC'
            when cc.standardized_country_code in ('BR', 'AR', 'CL', 'CO', 'PE') then 'LATIN_AMERICA'
            when cc.standardized_country_code in ('AE', 'SA', 'IL') then 'MIDDLE_EAST'
            when cc.standardized_country_code in ('ZA', 'NG', 'KE') then 'AFRICA'
            else 'OTHER'
        end as primary_geographic_region,
        
        -- Set primary allocation to 100% for CRM data (headquarters location)
        100.0 as allocation_percentage,
        true as is_primary_geography,
        'HEADQUARTERS' as geography_type,
        'CRM_VENDOR' as source_system

    from crm_companies cc
    where cc.standardized_country_code is not null
),

-- Extract geographic information from PM investments
pm_investment_geography as (
    select
        pi.company_id as source_company_id,
        pi.company_name,
        
        -- Parse geographic focus from PM data
        case 
            when upper(pi.geography) like '%US%' or upper(pi.geography) like '%UNITED STATES%' then 'US'
            when upper(pi.geography) like '%CANADA%' then 'CA'
            when upper(pi.geography) like '%UK%' or upper(pi.geography) like '%UNITED KINGDOM%' then 'GB'
            when upper(pi.geography) like '%GERMANY%' then 'DE'
            when upper(pi.geography) like '%FRANCE%' then 'FR'
            when upper(pi.geography) like '%JAPAN%' then 'JP'
            when upper(pi.geography) like '%CHINA%' then 'CN'
            when upper(pi.geography) like '%SINGAPORE%' then 'SG'
            when upper(pi.geography) like '%AUSTRALIA%' then 'AU'
            else null
        end as primary_country_code,
        
        null as state_province,
        null as city,
        
        pi.standardized_geography as primary_geographic_region,
        
        -- For PM data, assume 100% allocation to stated geography
        100.0 as allocation_percentage,
        true as is_primary_geography,
        'BUSINESS_OPERATIONS' as geography_type,
        'PORTFOLIO_MGMT_VENDOR' as source_system

    from pm_investments pi
    where pi.standardized_geography is not null
        and pi.standardized_geography != 'OTHER'
),

-- Combine geographic data from all sources
all_company_geography as (
    select * from crm_company_geography
    union all
    select * from pm_investment_geography
),

-- Resolve entities and create relationships
company_geography_relationships as (
    select
        -- Generate relationship identifier
        'REL-CG-' || coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || acg.source_company_id) || '-' || 
        coalesce(acg.primary_country_code, acg.primary_geographic_region) || '-' || acg.geography_type as relationship_id,
        'COMPANY_GEOGRAPHY' as relationship_type,
        acg.source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || acg.source_company_id) as canonical_company_id,
        acg.source_company_id,
        acg.company_name,
        cx.canonical_company_name,
        
        -- Geographic identifiers
        acg.primary_country_code as country_code,
        acg.primary_geographic_region as geographic_region,
        rc.country_name,
        rc.region as country_region,
        rc.sub_region as country_sub_region,
        
        -- Geographic relationship details
        acg.allocation_percentage,
        acg.is_primary_geography,
        acg.geography_type,
        
        -- Additional geographic context
        max(acg.state_province) as state_province,
        max(acg.city) as city,
        
        CURRENT_TIMESTAMP() as processed_at

    from all_company_geography acg
    left join company_xref cx on acg.source_company_id = cx.crm_company_id or acg.source_company_id = cx.pm_company_id
    left join ref_countries rc on acg.primary_country_code = rc.country_code
    group by 
        acg.source_company_id, acg.company_name, acg.primary_country_code, acg.primary_geographic_region,
        acg.geography_type, acg.allocation_percentage, acg.is_primary_geography, acg.source_system,
        cx.canonical_company_id, cx.canonical_company_name, rc.country_name, rc.region, rc.sub_region
),

-- Deduplicate and consolidate geographic relationships
consolidated_relationships as (
    select
        min(relationship_id) as relationship_id,
        relationship_type,
        canonical_company_id,
        source_company_id,
        company_name,
        canonical_company_name,
        
        -- Geographic identifiers
        country_code,
        geographic_region,
        country_name,
        country_region,
        country_sub_region,
        
        -- Consolidate allocation percentages (sum if multiple sources)
        sum(allocation_percentage) as total_allocation_percentage,
        
        -- Determine primary geography (prefer CRM data)
        max(case when is_primary_geography then 1 else 0 end) = 1 as is_primary_geography,
        
        -- Consolidate geography types
        listagg(distinct geography_type, ', ') as geography_types,
        
        -- Source system tracking
        listagg(distinct source_system, ', ') as source_systems,
        count(*) as source_record_count,
        
        -- Geographic details (aggregate from company_geography_relationships)
        max(state_province) as state_province,
        max(city) as city,
        
        processed_at

    from company_geography_relationships
    group by 
        relationship_type, canonical_company_id, source_company_id,
        company_name, canonical_company_name, country_code, geographic_region,
        country_name, country_region, country_sub_region, processed_at
),

-- Add enhanced geographic analytics
enhanced_relationships as (
    select
        *,
        
        -- Normalize allocation percentages (cap at 100%)
        case 
            when total_allocation_percentage > 100 then 100.0
            else total_allocation_percentage
        end as normalized_allocation_percentage,
        
        -- Geographic risk assessment
        case 
            when geographic_region = 'NORTH_AMERICA' then 'LOW_RISK'
            when geographic_region = 'EUROPE' then 'LOW_MODERATE_RISK'
            when geographic_region = 'ASIA_PACIFIC' and country_code in ('JP', 'AU', 'SG') then 'MODERATE_RISK'
            when geographic_region = 'ASIA_PACIFIC' then 'MODERATE_HIGH_RISK'
            when geographic_region = 'LATIN_AMERICA' then 'HIGH_RISK'
            when geographic_region in ('MIDDLE_EAST', 'AFRICA') then 'HIGH_RISK'
            else 'UNKNOWN_RISK'
        end as geographic_risk_level,
        
        -- Market development assessment
        case 
            when country_code in ('US', 'CA', 'GB', 'DE', 'FR', 'JP', 'AU') then 'DEVELOPED_MARKET'
            when country_code in ('CN', 'IN', 'BR', 'MX', 'KR', 'SG') then 'EMERGING_MARKET'
            when geographic_region in ('LATIN_AMERICA', 'MIDDLE_EAST', 'AFRICA') then 'FRONTIER_MARKET'
            else 'UNKNOWN_MARKET'
        end as market_development_level,
        
        -- Currency risk assessment
        case 
            when country_code in ('US') then 'NO_CURRENCY_RISK'  -- Assuming USD base
            when country_code in ('CA', 'GB', 'DE', 'FR', 'JP', 'AU', 'CH') then 'LOW_CURRENCY_RISK'
            when country_code in ('CN', 'KR', 'SG', 'HK') then 'MODERATE_CURRENCY_RISK'
            when geographic_region in ('LATIN_AMERICA', 'MIDDLE_EAST', 'AFRICA') then 'HIGH_CURRENCY_RISK'
            else 'UNKNOWN_CURRENCY_RISK'
        end as currency_risk_level,
        
        -- Regulatory environment assessment
        case 
            when country_code in ('US', 'CA', 'GB', 'DE', 'FR', 'AU', 'CH', 'NL', 'SE') then 'STABLE_REGULATORY'
            when country_code in ('JP', 'SG', 'HK') then 'STABLE_REGULATORY'
            when country_code in ('CN', 'IN', 'BR', 'MX') then 'EVOLVING_REGULATORY'
            when geographic_region in ('LATIN_AMERICA', 'MIDDLE_EAST', 'AFRICA') then 'UNCERTAIN_REGULATORY'
            else 'UNKNOWN_REGULATORY'
        end as regulatory_environment,
        
        -- Geographic diversification benefit
        case 
            when is_primary_geography = true and normalized_allocation_percentage >= 80 then 'CONCENTRATED_GEOGRAPHY'
            when is_primary_geography = true and normalized_allocation_percentage >= 60 then 'DOMINANT_GEOGRAPHY'
            when is_primary_geography = true and normalized_allocation_percentage >= 40 then 'PRIMARY_GEOGRAPHY'
            when normalized_allocation_percentage >= 20 then 'SIGNIFICANT_GEOGRAPHY'
            when normalized_allocation_percentage >= 5 then 'MINOR_GEOGRAPHY'
            else 'MINIMAL_GEOGRAPHY'
        end as geographic_significance,
        
        -- Time zone considerations for operations
        case 
            when country_code in ('US', 'CA', 'MX') then 'AMERICAS_TIMEZONE'
            when country_code in ('GB', 'DE', 'FR', 'IT', 'ES', 'NL', 'CH') then 'EUROPE_TIMEZONE'
            when country_code in ('JP', 'CN', 'KR', 'SG', 'HK', 'AU') then 'ASIA_PACIFIC_TIMEZONE'
            else 'OTHER_TIMEZONE'
        end as operational_timezone,
        
        -- Data source reliability
        case 
            when source_record_count >= 2 and ARRAY_CONTAINS('CRM_VENDOR'::variant, split(source_systems, ', ')) then 'HIGH_RELIABILITY'
            when ARRAY_CONTAINS('CRM_VENDOR'::variant, split(source_systems, ', ')) then 'MEDIUM_RELIABILITY'
            when source_record_count >= 2 then 'MEDIUM_RELIABILITY'
            else 'LOW_RELIABILITY'
        end as data_reliability

    from consolidated_relationships
),

-- Final relationship preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall geographic relationship quality score (0-100)
        (
            case when normalized_allocation_percentage >= 50 then 25
                 when normalized_allocation_percentage >= 20 then 20
                 when normalized_allocation_percentage >= 10 then 15
                 when normalized_allocation_percentage >= 5 then 10
                 else 5 end +
            case when geographic_risk_level = 'LOW_RISK' then 20
                 when geographic_risk_level = 'LOW_MODERATE_RISK' then 17
                 when geographic_risk_level = 'MODERATE_RISK' then 14
                 when geographic_risk_level = 'MODERATE_HIGH_RISK' then 10
                 when geographic_risk_level = 'HIGH_RISK' then 5
                 else 0 end +
            case when market_development_level = 'DEVELOPED_MARKET' then 20
                 when market_development_level = 'EMERGING_MARKET' then 15
                 when market_development_level = 'FRONTIER_MARKET' then 8
                 else 0 end +
            case when currency_risk_level = 'NO_CURRENCY_RISK' then 15
                 when currency_risk_level = 'LOW_CURRENCY_RISK' then 12
                 when currency_risk_level = 'MODERATE_CURRENCY_RISK' then 8
                 when currency_risk_level = 'HIGH_CURRENCY_RISK' then 3
                 else 0 end +
            case when regulatory_environment = 'STABLE_REGULATORY' then 15
                 when regulatory_environment = 'EVOLVING_REGULATORY' then 10
                 when regulatory_environment = 'UNCERTAIN_REGULATORY' then 5
                 else 0 end +
            case when data_reliability = 'HIGH_RELIABILITY' then 5
                 when data_reliability = 'MEDIUM_RELIABILITY' then 3
                 else 0 end
        ) as geographic_relationship_quality_score,
        
        -- Investment risk contribution
        case 
            when geographic_relationship_quality_score >= 85 and normalized_allocation_percentage >= 50 then 'LOW_RISK_CONTRIBUTION'
            when geographic_relationship_quality_score >= 70 and normalized_allocation_percentage >= 20 then 'MODERATE_RISK_CONTRIBUTION'
            when geographic_relationship_quality_score >= 50 then 'MEDIUM_RISK_CONTRIBUTION'
            when geographic_relationship_quality_score >= 30 then 'HIGH_RISK_CONTRIBUTION'
            else 'VERY_HIGH_RISK_CONTRIBUTION'
        end as investment_risk_contribution,
        
        -- Portfolio diversification impact
        case 
            when geographic_significance = 'CONCENTRATED_GEOGRAPHY' and geographic_risk_level in ('MODERATE_HIGH_RISK', 'HIGH_RISK') then 'CONCENTRATION_RISK'
            when geographic_significance in ('DOMINANT_GEOGRAPHY', 'PRIMARY_GEOGRAPHY') and market_development_level = 'DEVELOPED_MARKET' then 'STABLE_CONCENTRATION'
            when geographic_significance in ('SIGNIFICANT_GEOGRAPHY', 'MINOR_GEOGRAPHY') then 'DIVERSIFICATION_BENEFIT'
            else 'NEUTRAL_IMPACT'
        end as diversification_impact,
        
        -- Monitoring priority for geographic exposure
        case 
            when investment_risk_contribution in ('HIGH_RISK_CONTRIBUTION', 'VERY_HIGH_RISK_CONTRIBUTION') 
                and normalized_allocation_percentage >= 25 then 'HIGH_PRIORITY'
            when geographic_risk_level = 'HIGH_RISK' 
                and normalized_allocation_percentage >= 10 then 'MEDIUM_PRIORITY'
            when currency_risk_level = 'HIGH_CURRENCY_RISK' 
                and normalized_allocation_percentage >= 15 then 'MEDIUM_PRIORITY'
            else 'LOW_PRIORITY'
        end as monitoring_priority,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(relationship_id, canonical_company_id, country_code, geographic_region, normalized_allocation_percentage, is_primary_geography, geography_types, source_systems)) as record_hash

    from enhanced_relationships
)

select * from final