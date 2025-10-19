{{
  config(
    materialized='table',
    tags=['intermediate', 'relationships']
  )
}}

/*
  Intermediate model for company-industry relationship preparation
  
  This model prepares company-industry relationships from multiple data sources,
  creating bridge table records that capture industry exposure and classification
  for portfolio companies across different industry sectors and sub-sectors.
  
  Data sources:
  - CRM companies (primary industry data)
  - Portfolio management investments (sector classification)
  - Company cross-reference mappings for entity resolution
  - Reference industry data for standardization
  
  Business logic:
  - Create company-industry relationships with allocation percentages
  - Handle primary vs secondary industry exposures
  - Standardize industry classifications and hierarchies
  - Prepare industry relationship data for canonical bridge tables
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

ref_industries as (
    select * from {{ ref('stg_ref_industries') }}
),

-- Extract industry information from CRM companies
crm_company_industry as (
    select
        cc.company_id as source_company_id,
        cc.company_name,
        cc.industry_primary,
        cc.industry_secondary,
        cc.industry_sector as standardized_sector,
        
        -- Create primary industry relationship
        cc.industry_primary as industry_name,
        cc.industry_sector as industry_sector,
        80.0 as allocation_percentage,  -- Primary industry gets 80%
        true as is_primary_industry,
        'PRIMARY' as industry_classification,
        'CRM_VENDOR' as source_system,
        1 as industry_rank

    from crm_companies cc
    where cc.industry_primary is not null

    union all

    -- Create secondary industry relationship if exists
    select
        cc.company_id as source_company_id,
        cc.company_name,
        cc.industry_primary,
        cc.industry_secondary,
        cc.industry_sector as standardized_sector,
        
        cc.industry_secondary as industry_name,
        -- Map secondary industry to sector (simplified mapping)
        case 
            when upper(cc.industry_secondary) like '%TECHNOLOGY%' or upper(cc.industry_secondary) like '%SOFTWARE%' then 'TECHNOLOGY'
            when upper(cc.industry_secondary) like '%HEALTHCARE%' or upper(cc.industry_secondary) like '%MEDICAL%' then 'HEALTHCARE'
            when upper(cc.industry_secondary) like '%FINANCIAL%' or upper(cc.industry_secondary) like '%FINTECH%' then 'FINANCIAL_SERVICES'
            when upper(cc.industry_secondary) like '%CONSUMER%' or upper(cc.industry_secondary) like '%RETAIL%' then 'CONSUMER'
            when upper(cc.industry_secondary) like '%INDUSTRIAL%' or upper(cc.industry_secondary) like '%MANUFACTURING%' then 'INDUSTRIALS'
            when upper(cc.industry_secondary) like '%ENERGY%' or upper(cc.industry_secondary) like '%RENEWABLE%' then 'ENERGY'
            when upper(cc.industry_secondary) like '%REAL ESTATE%' then 'REAL_ESTATE'
            else 'OTHER'
        end as industry_sector,
        20.0 as allocation_percentage,  -- Secondary industry gets 20%
        false as is_primary_industry,
        'SECONDARY' as industry_classification,
        'CRM_VENDOR' as source_system,
        2 as industry_rank

    from crm_companies cc
    where cc.industry_secondary is not null
        and cc.industry_secondary != cc.industry_primary
),

-- Extract industry information from PM investments
pm_investment_industry as (
    select
        pi.company_id as source_company_id,
        pi.company_name,
        pi.sector as industry_name,
        pi.standardized_sector as industry_sector,
        
        -- For PM data, assume 100% allocation to stated sector
        100.0 as allocation_percentage,
        true as is_primary_industry,
        'PRIMARY' as industry_classification,
        'PORTFOLIO_MGMT_VENDOR' as source_system,
        1 as industry_rank

    from pm_investments pi
    where pi.standardized_sector is not null
        and pi.standardized_sector != 'OTHER'
),

-- Combine industry data from all sources
all_company_industry as (
    select 
        source_company_id,
        company_name,
        industry_name,
        industry_sector,
        allocation_percentage,
        is_primary_industry,
        industry_classification,
        source_system,
        industry_rank
    from crm_company_industry
    
    union all
    
    select 
        source_company_id,
        company_name,
        industry_name,
        industry_sector,
        allocation_percentage,
        is_primary_industry,
        industry_classification,
        source_system,
        industry_rank
    from pm_investment_industry
),

-- Resolve entities and create relationships
company_industry_relationships as (
    select
        -- Generate relationship identifier
        'REL-CI-' || coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || aci.source_company_id) || '-' || 
        aci.industry_sector || '-' || aci.industry_classification as relationship_id,
        'COMPANY_INDUSTRY' as relationship_type,
        aci.source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || aci.source_company_id) as canonical_company_id,
        aci.source_company_id,
        aci.company_name,
        cx.canonical_company_name,
        
        -- Industry identifiers and details
        aci.industry_name,
        aci.industry_sector,
        aci.industry_sector,
        ri.industry_code,
        ri.gics_industry_group as industry_group,
        null as industry_description,
        ri.gics_sector,
        ri.gics_industry_group,
        null as naics_code,
        
        -- Industry relationship details
        aci.allocation_percentage,
        aci.is_primary_industry,
        aci.industry_classification,
        aci.industry_rank,
        
        CURRENT_TIMESTAMP() as processed_at

    from all_company_industry aci
    left join company_xref cx on aci.source_company_id = cx.crm_company_id or aci.source_company_id = cx.pm_company_id
    left join ref_industries ri on upper(aci.industry_sector) = upper(ri.industry_name)
),

-- Deduplicate and consolidate industry relationships
consolidated_relationships as (
    select
        -- Use the minimum relationship_id for the group
        min(relationship_id) as relationship_id,
        
        relationship_type,
        canonical_company_id,
        source_company_id,
        company_name,
        canonical_company_name,
        
        -- Industry identifiers
        max(industry_sector) as industry_sector,
        industry_classification,
        
        -- Consolidate industry names (use first non-null)
        max(industry_name) as industry_name,
        
        -- Reference data (take first non-null values)
        first_value(industry_code ignore nulls) over (
            partition by canonical_company_id, industry_sector, industry_classification
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
        ) as industry_code,
        
        first_value(industry_group ignore nulls) over (
            partition by canonical_company_id, industry_sector, industry_classification
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
        ) as industry_group,
        
        first_value(industry_description ignore nulls) over (
            partition by canonical_company_id, industry_sector, industry_classification
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
        ) as industry_description,
        
        first_value(gics_sector ignore nulls) over (
            partition by canonical_company_id, industry_sector, industry_classification
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
        ) as gics_sector,
        
        first_value(gics_industry_group ignore nulls) over (
            partition by canonical_company_id, industry_sector, industry_classification
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
        ) as gics_industry_group,
        
        first_value(naics_code ignore nulls) over (
            partition by canonical_company_id, industry_sector, industry_classification
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
        ) as naics_code,
        
        -- Consolidate allocation percentages (average if multiple sources)
        avg(allocation_percentage) as avg_allocation_percentage,
        
        -- Determine primary industry (prefer CRM data)
        max(case when is_primary_industry then 1 else 0 end) = 1 as is_primary_industry,
        
        -- Industry rank (minimum rank wins)
        min(industry_rank) as industry_rank,
        
        -- Source system tracking
        listagg(distinct source_system, ', ') as source_systems,
        count(*) as source_record_count,
        
        processed_at

    from company_industry_relationships
    group by 
        canonical_company_id, source_company_id, company_name, canonical_company_name,
        industry_classification, relationship_type, processed_at
),

-- Remove duplicates by selecting one record per group
deduplicated_relationships as (
    select distinct
        relationship_id,
        relationship_type,
        canonical_company_id,
        source_company_id,
        company_name,
        canonical_company_name,
        industry_name,
        industry_sector,
        industry_code,
        industry_group,
        industry_description,
        gics_sector,
        gics_industry_group,
        naics_code,
        avg_allocation_percentage as allocation_percentage,
        is_primary_industry,
        industry_classification,
        industry_rank,
        source_systems,
        source_record_count,
        processed_at
    from consolidated_relationships
),

-- Add enhanced industry analytics
enhanced_relationships as (
    select
        *,
        
        -- Normalize allocation percentages
        case 
            when allocation_percentage > 100 then 100.0
            when allocation_percentage < 0 then 0.0
            else allocation_percentage
        end as normalized_allocation_percentage,
        
        -- Industry risk assessment
        case 
            when industry_sector = 'TECHNOLOGY' then 'HIGH_VOLATILITY'
            when industry_sector = 'HEALTHCARE' then 'MODERATE_VOLATILITY'
            when industry_sector = 'FINANCIAL_SERVICES' then 'CYCLICAL_RISK'
            when industry_sector = 'CONSUMER' then 'CONSUMER_DEPENDENT'
            when industry_sector = 'INDUSTRIALS' then 'ECONOMIC_SENSITIVE'
            when industry_sector = 'ENERGY' then 'COMMODITY_RISK'
            when industry_sector = 'REAL_ESTATE' then 'INTEREST_RATE_SENSITIVE'
            else 'UNKNOWN_RISK'
        end as industry_risk_profile,
        
        -- Growth potential assessment
        case 
            when industry_sector = 'TECHNOLOGY' then 'HIGH_GROWTH_POTENTIAL'
            when industry_sector = 'HEALTHCARE' then 'STABLE_GROWTH_POTENTIAL'
            when industry_sector = 'FINANCIAL_SERVICES' then 'MODERATE_GROWTH_POTENTIAL'
            when industry_sector = 'CONSUMER' then 'MODERATE_GROWTH_POTENTIAL'
            when industry_sector = 'INDUSTRIALS' then 'CYCLICAL_GROWTH_POTENTIAL'
            when industry_sector = 'ENERGY' then 'VOLATILE_GROWTH_POTENTIAL'
            when industry_sector = 'REAL_ESTATE' then 'STABLE_GROWTH_POTENTIAL'
            else 'UNKNOWN_GROWTH_POTENTIAL'
        end as growth_potential,
        
        -- Regulatory environment assessment
        case 
            when industry_sector = 'HEALTHCARE' then 'HIGHLY_REGULATED'
            when industry_sector = 'FINANCIAL_SERVICES' then 'HIGHLY_REGULATED'
            when industry_sector = 'ENERGY' then 'REGULATED'
            when industry_sector = 'TECHNOLOGY' then 'EMERGING_REGULATION'
            when industry_sector = 'CONSUMER' then 'MODERATELY_REGULATED'
            when industry_sector = 'INDUSTRIALS' then 'MODERATELY_REGULATED'
            when industry_sector = 'REAL_ESTATE' then 'REGULATED'
            else 'UNKNOWN_REGULATION'
        end as regulatory_environment,
        
        -- Market maturity assessment
        case 
            when industry_sector = 'TECHNOLOGY' then 'RAPIDLY_EVOLVING'
            when industry_sector = 'HEALTHCARE' then 'MATURE_WITH_INNOVATION'
            when industry_sector = 'FINANCIAL_SERVICES' then 'MATURE_MARKET'
            when industry_sector = 'CONSUMER' then 'MATURE_MARKET'
            when industry_sector = 'INDUSTRIALS' then 'MATURE_MARKET'
            when industry_sector = 'ENERGY' then 'TRANSITIONING_MARKET'
            when industry_sector = 'REAL_ESTATE' then 'MATURE_MARKET'
            else 'UNKNOWN_MATURITY'
        end as market_maturity,
        
        -- ESG considerations
        case 
            when industry_sector = 'ENERGY' then 'HIGH_ESG_SCRUTINY'
            when industry_sector = 'INDUSTRIALS' then 'MODERATE_ESG_SCRUTINY'
            when industry_sector = 'TECHNOLOGY' then 'EMERGING_ESG_FOCUS'
            when industry_sector = 'HEALTHCARE' then 'POSITIVE_ESG_IMPACT'
            when industry_sector = 'FINANCIAL_SERVICES' then 'ESG_ENABLER'
            when industry_sector = 'CONSUMER' then 'VARIABLE_ESG_IMPACT'
            when industry_sector = 'REAL_ESTATE' then 'MODERATE_ESG_FOCUS'
            else 'UNKNOWN_ESG_IMPACT'
        end as esg_considerations,
        
        -- Industry significance assessment
        case 
            when is_primary_industry = true and normalized_allocation_percentage >= 80 then 'PURE_PLAY'
            when is_primary_industry = true and normalized_allocation_percentage >= 60 then 'DOMINANT_EXPOSURE'
            when is_primary_industry = true and normalized_allocation_percentage >= 40 then 'PRIMARY_EXPOSURE'
            when normalized_allocation_percentage >= 20 then 'SIGNIFICANT_EXPOSURE'
            when normalized_allocation_percentage >= 5 then 'MINOR_EXPOSURE'
            else 'MINIMAL_EXPOSURE'
        end as industry_significance,
        
        -- Data source reliability
        case 
            when source_record_count >= 2 and ARRAY_CONTAINS('CRM_VENDOR'::variant, split(source_systems, ', ')) then 'HIGH_RELIABILITY'
            when ARRAY_CONTAINS('CRM_VENDOR'::variant, split(source_systems, ', ')) then 'MEDIUM_RELIABILITY'
            when source_record_count >= 2 then 'MEDIUM_RELIABILITY'
            else 'LOW_RELIABILITY'
        end as data_reliability

    from deduplicated_relationships
),

-- Final relationship preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall industry relationship quality score (0-100)
        (
            case when normalized_allocation_percentage >= 50 then 25
                 when normalized_allocation_percentage >= 20 then 20
                 when normalized_allocation_percentage >= 10 then 15
                 when normalized_allocation_percentage >= 5 then 10
                 else 5 end +
            case when growth_potential = 'HIGH_GROWTH_POTENTIAL' then 20
                 when growth_potential = 'STABLE_GROWTH_POTENTIAL' then 17
                 when growth_potential = 'MODERATE_GROWTH_POTENTIAL' then 14
                 when growth_potential = 'CYCLICAL_GROWTH_POTENTIAL' then 10
                 when growth_potential = 'VOLATILE_GROWTH_POTENTIAL' then 7
                 else 0 end +
            case when industry_risk_profile = 'HIGH_VOLATILITY' then 10  -- High risk can mean high reward
                 when industry_risk_profile = 'MODERATE_VOLATILITY' then 15
                 when industry_risk_profile = 'CYCLICAL_RISK' then 12
                 when industry_risk_profile = 'CONSUMER_DEPENDENT' then 14
                 when industry_risk_profile = 'ECONOMIC_SENSITIVE' then 13
                 when industry_risk_profile = 'COMMODITY_RISK' then 8
                 when industry_risk_profile = 'INTEREST_RATE_SENSITIVE' then 11
                 else 0 end +
            case when market_maturity = 'RAPIDLY_EVOLVING' then 15
                 when market_maturity = 'MATURE_WITH_INNOVATION' then 18
                 when market_maturity = 'MATURE_MARKET' then 15
                 when market_maturity = 'TRANSITIONING_MARKET' then 10
                 else 0 end +
            case when esg_considerations in ('POSITIVE_ESG_IMPACT', 'ESG_ENABLER') then 15
                 when esg_considerations = 'EMERGING_ESG_FOCUS' then 12
                 when esg_considerations = 'VARIABLE_ESG_IMPACT' then 8
                 when esg_considerations = 'MODERATE_ESG_FOCUS' then 10
                 when esg_considerations = 'MODERATE_ESG_SCRUTINY' then 7
                 when esg_considerations = 'HIGH_ESG_SCRUTINY' then 3
                 else 5 end +
            case when data_reliability = 'HIGH_RELIABILITY' then 5
                 when data_reliability = 'MEDIUM_RELIABILITY' then 3
                 else 0 end
        ) as industry_relationship_quality_score,
        
        -- Investment attractiveness from industry perspective
        case 
            when industry_relationship_quality_score >= 85 and normalized_allocation_percentage >= 50 then 'HIGHLY_ATTRACTIVE_SECTOR'
            when industry_relationship_quality_score >= 70 and normalized_allocation_percentage >= 20 then 'ATTRACTIVE_SECTOR'
            when industry_relationship_quality_score >= 55 then 'NEUTRAL_SECTOR'
            when industry_relationship_quality_score >= 40 then 'CHALLENGING_SECTOR'
            else 'UNATTRACTIVE_SECTOR'
        end as sector_attractiveness,
        
        -- Portfolio diversification impact
        case 
            when industry_significance = 'PURE_PLAY' and industry_risk_profile in ('HIGH_VOLATILITY', 'COMMODITY_RISK') then 'CONCENTRATION_RISK'
            when industry_significance in ('DOMINANT_EXPOSURE', 'PRIMARY_EXPOSURE') and growth_potential = 'HIGH_GROWTH_POTENTIAL' then 'GROWTH_CONCENTRATION'
            when industry_significance in ('SIGNIFICANT_EXPOSURE', 'MINOR_EXPOSURE') then 'DIVERSIFICATION_BENEFIT'
            else 'NEUTRAL_IMPACT'
        end as diversification_impact,
        
        -- Monitoring priority for industry exposure
        case 
            when industry_risk_profile in ('HIGH_VOLATILITY', 'COMMODITY_RISK') 
                and normalized_allocation_percentage >= 25 then 'HIGH_PRIORITY'
            when regulatory_environment = 'HIGHLY_REGULATED' 
                and normalized_allocation_percentage >= 15 then 'HIGH_PRIORITY'
            when esg_considerations = 'HIGH_ESG_SCRUTINY' 
                and normalized_allocation_percentage >= 20 then 'MEDIUM_PRIORITY'
            when growth_potential = 'VOLATILE_GROWTH_POTENTIAL' 
                and normalized_allocation_percentage >= 10 then 'MEDIUM_PRIORITY'
            else 'LOW_PRIORITY'
        end as monitoring_priority,
        
        -- Thematic investment alignment
        case 
            when industry_sector = 'TECHNOLOGY' and gics_industry_group like '%Software%' then 'DIGITAL_TRANSFORMATION'
            when industry_sector = 'HEALTHCARE' and gics_industry_group like '%Biotechnology%' then 'LIFE_SCIENCES_INNOVATION'
            when industry_sector = 'ENERGY' and industry_name like '%Renewable%' then 'CLEAN_ENERGY_TRANSITION'
            when industry_sector = 'FINANCIAL_SERVICES' and industry_name like '%Fintech%' then 'FINANCIAL_INNOVATION'
            when industry_sector = 'CONSUMER' and industry_name like '%E-commerce%' then 'DIGITAL_COMMERCE'
            else 'TRADITIONAL_SECTORS'
        end as thematic_alignment,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(relationship_id, canonical_company_id, industry_sector, industry_name, normalized_allocation_percentage, is_primary_industry, industry_classification, source_systems)) as record_hash

    from enhanced_relationships
)

select * from final