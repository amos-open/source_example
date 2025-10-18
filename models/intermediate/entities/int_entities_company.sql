{{
  config(
    materialized='table',
    tags=['intermediate', 'entities']
  )
}}

/*
  Intermediate model for company entity preparation
  
  This model combines company data from multiple source systems using cross-reference mappings
  to create unified company entities with comprehensive entity resolution logic.
  
  Data sources:
  - CRM system (primary for company profiles)
  - Portfolio management system (financial and operational data)
  - Accounting system (financial transactions)
  - Cross-reference mappings for entity resolution
  
  Business logic:
  - Prioritize CRM data for company profiles and contact information
  - Use PM data for financial performance and valuations
  - Apply sophisticated entity resolution and conflict handling
  - Generate comprehensive data quality scoring
*/

with crm_companies as (
    select * from {{ ref('stg_crm_companies') }}
),

pm_investments as (
    select * from {{ ref('stg_pm_investments') }}
),

pm_financials as (
    select * from {{ ref('stg_pm_company_financials') }}
),

pm_valuations as (
    select * from {{ ref('stg_pm_valuations') }}
),

xref_companies as (
    select * from {{ ref('stg_ref_xref_companies') }}
    where data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

-- Aggregate PM investment data by company
pm_company_summary as (
    select
        company_id as pm_company_id,
        company_name,
        count(*) as investment_count,
        sum(investment_amount) as total_investment_amount,
        min(investment_date) as first_investment_date,
        max(investment_date) as latest_investment_date,
        string_agg(distinct investment_type, ', ') as investment_types,
        avg(ownership_percentage) as avg_ownership_percentage,
        sum(case when investment_status = 'ACTIVE' then 1 else 0 end) as active_investments
    from pm_investments
    where company_id is not null
    group by company_id, company_name
),

-- Get latest financial metrics by company
pm_latest_financials as (
    select
        company_id as pm_company_id,
        revenue,
        ebitda,
        net_income,
        total_assets,
        total_debt,
        reporting_date,
        row_number() over (partition by company_id order by reporting_date desc) as rn
    from pm_financials
    where company_id is not null
        and reporting_date is not null
),

pm_current_financials as (
    select * from pm_latest_financials where rn = 1
),

-- Get latest valuation by company
pm_latest_valuations as (
    select
        company_id as pm_company_id,
        enterprise_value,
        equity_value,
        valuation_method,
        valuation_date,
        row_number() over (partition by company_id order by valuation_date desc) as rn
    from pm_valuations
    where company_id is not null
        and valuation_date is not null
),

pm_current_valuations as (
    select * from pm_latest_valuations where rn = 1
),

-- Combine company data using cross-reference mappings
unified_companies as (
    select
        x.canonical_company_id,
        
        -- Company identification (prioritize CRM system)
        coalesce(c.company_name, pms.company_name) as company_name,
        c.legal_name as company_legal_name,
        c.company_id as crm_company_id,
        x.pm_company_id,
        x.accounting_entity_id,
        
        -- Industry and classification (CRM primary)
        c.industry_primary,
        c.industry_secondary,
        c.industry_sector,
        
        -- Geographic information (CRM primary)
        c.standardized_country_code as country_code,
        c.state_province,
        c.city,
        
        -- Company characteristics (CRM primary)
        c.founded_year,
        c.employee_count,
        c.company_size_category,
        c.revenue_range_text,
        c.revenue_midpoint_millions,
        c.website_url,
        c.company_description,
        c.business_model,
        c.competitive_position,
        c.key_risks,
        
        -- ESG and quality scores (CRM)
        c.esg_score,
        c.data_quality_score as crm_data_quality_score,
        
        -- Investment summary (PM system)
        pms.investment_count,
        pms.total_investment_amount,
        pms.first_investment_date,
        pms.latest_investment_date,
        pms.investment_types,
        pms.avg_ownership_percentage,
        pms.active_investments,
        
        -- Latest financial data (PM system)
        pmf.revenue as latest_revenue,
        pmf.ebitda as latest_ebitda,
        pmf.net_income as latest_net_income,
        pmf.total_assets as latest_total_assets,
        pmf.total_debt as latest_total_debt,
        pmf.reporting_date as latest_financial_date,
        
        -- Latest valuation data (PM system)
        pmv.enterprise_value as latest_enterprise_value,
        pmv.equity_value as latest_equity_value,
        pmv.valuation_method as latest_valuation_method,
        pmv.valuation_date as latest_valuation_date,
        
        -- Cross-reference metadata
        x.standardized_confidence as resolution_confidence,
        x.source_systems_count,
        x.data_quality_rating as xref_data_quality,
        x.primary_source_system,
        
        -- Data quality and completeness (CRM)
        c.completeness_score as crm_completeness_score,
        c.overall_data_quality as crm_data_quality,
        
        -- Source system tracking
        case 
            when c.company_id is not null then 'CRM_VENDOR'
            when pms.pm_company_id is not null then 'PM_VENDOR'
            else 'UNKNOWN'
        end as primary_data_source,
        
        case 
            when c.company_id is not null and pms.pm_company_id is not null then 'MULTI_SOURCE'
            when c.company_id is not null then 'CRM_ONLY'
            when pms.pm_company_id is not null then 'PM_ONLY'
            else 'NO_SOURCE'
        end as source_coverage,
        
        -- Audit fields
        coalesce(c.created_date, pms.first_investment_date) as created_date,
        greatest(
            coalesce(c.last_modified_date, '1900-01-01'::date),
            coalesce(x.last_modified_date, '1900-01-01'::date),
            coalesce(pmf.reporting_date, '1900-01-01'::date),
            coalesce(pmv.valuation_date, '1900-01-01'::date)
        ) as last_modified_date,
        
        CURRENT_TIMESTAMP() as processed_at

    from xref_companies x
    left join crm_companies c on x.crm_company_id = c.company_id
    left join pm_company_summary pms on x.pm_company_id = pms.pm_company_id
    left join pm_current_financials pmf on x.pm_company_id = pmf.pm_company_id
    left join pm_current_valuations pmv on x.pm_company_id = pmv.pm_company_id
),

-- Add derived metrics and validation
enhanced_companies as (
    select
        *,
        
        -- Company lifecycle stage
        case 
            when founded_year is null then 'UNKNOWN'
            when EXTRACT(YEAR FROM CURRENT_DATE()) - founded_year < 5 then 'STARTUP'
            when EXTRACT(YEAR FROM CURRENT_DATE()) - founded_year < 10 then 'GROWTH'
            when EXTRACT(YEAR FROM CURRENT_DATE()) - founded_year < 20 then 'MATURE'
            else 'ESTABLISHED'
        end as company_lifecycle_stage,
        
        -- Investment status
        case 
            when active_investments > 0 then 'PORTFOLIO_COMPANY'
            when investment_count > 0 then 'FORMER_PORTFOLIO'
            else 'PROSPECT'
        end as investment_status,
        
        -- Financial health indicators
        case 
            when latest_ebitda is not null and latest_revenue is not null and latest_revenue > 0 then
                latest_ebitda / latest_revenue * 100
            else null
        end as ebitda_margin_percentage,
        
        case 
            when latest_total_debt is not null and latest_total_assets is not null and latest_total_assets > 0 then
                latest_total_debt / latest_total_assets * 100
            else null
        end as debt_to_assets_ratio,
        
        -- Valuation metrics
        case 
            when latest_enterprise_value is not null and latest_revenue is not null and latest_revenue > 0 then
                latest_enterprise_value / latest_revenue
            else null
        end as ev_revenue_multiple,
        
        case 
            when latest_enterprise_value is not null and latest_ebitda is not null and latest_ebitda > 0 then
                latest_enterprise_value / latest_ebitda
            else null
        end as ev_ebitda_multiple,
        
        -- Investment performance indicators
        case 
            when first_investment_date is not null then
                DATE_DIFF(current_date(), first_investment_date, YEAR)
            else null
        end as investment_duration_years,
        
        case 
            when total_investment_amount is not null and latest_equity_value is not null and total_investment_amount > 0 then
                (latest_equity_value - total_investment_amount) / total_investment_amount * 100
            else null
        end as unrealized_return_percentage,
        
        -- Data freshness assessment
        case 
            when latest_financial_date is not null then
                DATE_DIFF(current_date(), latest_financial_date, MONTH)
            else null
        end as financial_data_age_months,
        
        case 
            when latest_valuation_date is not null then
                DATE_DIFF(current_date(), latest_valuation_date, MONTH)
            else null
        end as valuation_data_age_months,
        
        -- Overall data completeness score
        (
            case when company_name is not null then 1 else 0 end +
            case when industry_primary is not null then 1 else 0 end +
            case when country_code is not null then 1 else 0 end +
            case when founded_year is not null then 1 else 0 end +
            case when employee_count is not null then 1 else 0 end +
            case when revenue_midpoint_millions is not null then 1 else 0 end +
            case when website_url is not null then 1 else 0 end +
            case when company_description is not null then 1 else 0 end +
            case when latest_revenue is not null then 1 else 0 end +
            case when latest_enterprise_value is not null then 1 else 0 end
        ) / 10.0 * 100 as overall_completeness_score,
        
        -- Financial data completeness
        (
            case when latest_revenue is not null then 1 else 0 end +
            case when latest_ebitda is not null then 1 else 0 end +
            case when latest_net_income is not null then 1 else 0 end +
            case when latest_total_assets is not null then 1 else 0 end +
            case when latest_enterprise_value is not null then 1 else 0 end
        ) / 5.0 * 100 as financial_completeness_score

    from unified_companies
),

-- Final quality assessment and scoring
final as (
    select
        -- Canonical model format - exact column names and types expected by amos_core
        CAST(canonical_company_id AS STRING) as id,
        CAST(company_name AS STRING) as name,
        CAST(website_url AS STRING) as website,
        CAST(company_description AS STRING) as description,
        CAST(COALESCE(country_code, 'USD') AS STRING) as currency,  -- Use country_code as currency fallback
        CAST(GENERATE_UUID() AS STRING) as industry_id,  -- Generate UUID for industry_id
        CAST(created_date AS TIMESTAMP) as created_at,
        CAST(last_modified_date AS TIMESTAMP) as updated_at,
        
        -- Additional intermediate fields for analysis (not used by canonical model)
        crm_company_id,
        pm_company_id,
        industry_primary,
        industry_secondary,
        country_code,
        founded_year,
        employee_count,
        company_size_category,
        latest_revenue,
        latest_ebitda,
        latest_enterprise_value,
        investment_status,
        
        -- Overall data quality assessment
        case 
            when resolution_confidence = 'HIGH' 
                and crm_data_quality = 'HIGH'
                and overall_completeness_score >= 90 
            then 'EXCELLENT'
            when resolution_confidence in ('HIGH', 'MEDIUM')
                and crm_data_quality in ('HIGH', 'MEDIUM')
                and overall_completeness_score >= 70
            then 'GOOD'
            when overall_completeness_score >= 50
            then 'FAIR'
            else 'POOR'
        end as overall_data_quality,
        
        -- Investment attractiveness score
        (
            case when company_size_category = 'ENTERPRISE' then 3
                 when company_size_category = 'LARGE' then 2
                 when company_size_category = 'MEDIUM' then 1
                 else 0 end +
            case when ebitda_margin_percentage >= 20 then 3
                 when ebitda_margin_percentage >= 10 then 2
                 when ebitda_margin_percentage >= 0 then 1
                 else 0 end +
            case when debt_to_assets_ratio <= 30 then 2
                 when debt_to_assets_ratio <= 50 then 1
                 else 0 end +
            case when company_lifecycle_stage in ('GROWTH', 'MATURE') then 2
                 when company_lifecycle_stage = 'STARTUP' then 1
                 else 0 end +
            case when industry_sector in ('TECHNOLOGY', 'HEALTHCARE', 'FINANCIAL_SERVICES') then 2
                 else 1 end
        ) as investment_attractiveness_score,
        
        -- Investment recommendation
        case 
            when investment_attractiveness_score >= 10 and overall_data_quality in ('EXCELLENT', 'GOOD') then 'HIGHLY_RECOMMENDED'
            when investment_attractiveness_score >= 7 and overall_data_quality in ('EXCELLENT', 'GOOD', 'FAIR') then 'RECOMMENDED'
            when investment_attractiveness_score >= 5 then 'CONSIDER'
            else 'NOT_RECOMMENDED'
        end as investment_recommendation,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(canonical_company_id, company_name, industry_primary, country_code, founded_year, employee_count, revenue_midpoint_millions, latest_revenue, latest_ebitda, latest_enterprise_value, last_modified_date)) as record_hash

    from enhanced_companies
    where canonical_company_id is not null
)

select * from final