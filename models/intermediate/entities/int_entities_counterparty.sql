{{
  config(
    materialized='table',
    tags=['intermediate', 'entities']
  )
}}

/*
  Intermediate model for counterparty entity preparation
  
  This model consolidates counterparty data from multiple source systems,
  creating unified counterparty entities for service providers, lenders,
  co-investors, and other external parties.
  
  Data sources:
  - CRM system (service providers, co-investors)
  - Portfolio management system (lenders, advisors)
  - Accounting system (vendors, service providers)
  - Cross-reference mappings for entity resolution
  
  Business logic:
  - Consolidate counterparty data across multiple source systems
  - Standardize counterparty types and classifications
  - Apply relationship scoring and categorization
  - Generate comprehensive counterparty profiling
*/

with crm_contacts as (
    select * from {{ ref('stg_crm_contacts') }}
),

-- Extract counterparty information from CRM contacts
crm_counterparties as (
    select
        contact_id as crm_counterparty_id,
        company_name as counterparty_name,
        contact_name as primary_contact_name,
        contact_title as primary_contact_title,
        email as primary_contact_email,
        phone as primary_contact_phone,
        
        -- Infer counterparty type from contact type and company name
        case 
            when upper(contact_type) like '%SERVICE%' or upper(company_name) like '%CONSULTING%' 
                or upper(company_name) like '%ADVISORY%' then 'SERVICE_PROVIDER'
            when upper(contact_type) like '%INVESTOR%' or upper(contact_type) like '%LP%' 
                or upper(contact_type) like '%LIMITED PARTNER%' then 'CO_INVESTOR'
            when upper(contact_type) like '%LENDER%' or upper(company_name) like '%BANK%' 
                or upper(company_name) like '%CREDIT%' then 'LENDER'
            when upper(contact_type) like '%VENDOR%' or upper(contact_type) like '%SUPPLIER%' then 'VENDOR'
            when upper(contact_type) like '%LEGAL%' or upper(company_name) like '%LAW%' 
                or upper(company_name) like '%LEGAL%' then 'LEGAL_COUNSEL'
            when upper(contact_type) like '%AUDIT%' or upper(company_name) like '%AUDIT%' 
                or upper(company_name) like '%ACCOUNTING%' then 'AUDITOR'
            else 'OTHER'
        end as counterparty_type,
        
        industry as counterparty_industry,
        country_code,
        relationship_status,
        relationship_strength,
        last_interaction_date,
        interaction_frequency,
        
        -- Source metadata
        'CRM_VENDOR' as source_system,
        created_date,
        last_modified_date

    from crm_contacts
    where company_name is not null
        and contact_type is not null
),

-- Extract counterparty information from PM investments (lenders, advisors)
pm_counterparties as (
    select
        'PM_' || row_number() over (order by company_name) as pm_counterparty_id,
        company_name as counterparty_name,
        null as primary_contact_name,
        null as primary_contact_title,
        null as primary_contact_email,
        null as primary_contact_phone,
        
        'LENDER' as counterparty_type,  -- PM system primarily tracks lenders
        'FINANCIAL_SERVICES' as counterparty_industry,
        null as country_code,
        'ACTIVE' as relationship_status,
        null as relationship_strength,
        max(investment_date) as last_interaction_date,
        null as interaction_frequency,
        
        'PM_VENDOR' as source_system,
        min(investment_date) as created_date,
        max(investment_date) as last_modified_date

    from {{ ref('stg_pm_investments') }}
    where debt_provider is not null
        and company_name != debt_provider  -- Exclude self-references
    group by company_name
),

-- Extract counterparty information from accounting (vendors, service providers)
acc_counterparties as (
    select
        'ACC_' || row_number() over (order by payee_name) as acc_counterparty_id,
        payee_name as counterparty_name,
        null as primary_contact_name,
        null as primary_contact_title,
        null as primary_contact_email,
        null as primary_contact_phone,
        
        case 
            when upper(account_name) like '%LEGAL%' or upper(payee_name) like '%LAW%' then 'LEGAL_COUNSEL'
            when upper(account_name) like '%AUDIT%' or upper(payee_name) like '%AUDIT%' then 'AUDITOR'
            when upper(account_name) like '%CONSULTING%' or upper(payee_name) like '%ADVISORY%' then 'SERVICE_PROVIDER'
            when upper(account_name) like '%BANK%' or upper(account_name) like '%INTEREST%' then 'LENDER'
            else 'VENDOR'
        end as counterparty_type,
        
        null as counterparty_industry,
        null as country_code,
        'ACTIVE' as relationship_status,
        null as relationship_strength,
        max(transaction_date) as last_interaction_date,
        null as interaction_frequency,
        
        'ACCOUNTING_VENDOR' as source_system,
        min(transaction_date) as created_date,
        max(transaction_date) as last_modified_date

    from {{ ref('stg_acc_journal_entries') }}
    where payee_name is not null
        and payee_name != 'INTERNAL'
        and amount < 0  -- Focus on payments to external parties
    group by payee_name, account_name
),

-- Combine all counterparty sources
all_counterparties as (
    select * from crm_counterparties
    union all
    select * from pm_counterparties
    union all
    select * from acc_counterparties
),

-- Deduplicate and consolidate counterparties by name
consolidated_counterparties as (
    select
        -- Generate canonical counterparty ID
        'CPTY-' || upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', '')) || '-' || 
        row_number() over (partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', '')) order by created_date) as canonical_counterparty_id,
        
        counterparty_name,
        
        -- Consolidate contact information (prioritize CRM data)
        first_value(primary_contact_name ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as primary_contact_name,
        
        first_value(primary_contact_title ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as primary_contact_title,
        
        first_value(primary_contact_email ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as primary_contact_email,
        
        first_value(primary_contact_phone ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as primary_contact_phone,
        
        -- Consolidate counterparty type (prioritize most specific)
        first_value(counterparty_type) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case 
                when counterparty_type in ('LEGAL_COUNSEL', 'AUDITOR', 'SERVICE_PROVIDER') then 1
                when counterparty_type in ('LENDER', 'CO_INVESTOR') then 2
                when counterparty_type = 'VENDOR' then 3
                else 4 end
            rows between unbounded preceding and unbounded following
        ) as counterparty_type,
        
        first_value(counterparty_industry ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as counterparty_industry,
        
        first_value(country_code ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as country_code,
        
        -- Relationship information
        first_value(relationship_status ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by last_modified_date desc
            rows between unbounded preceding and unbounded following
        ) as relationship_status,
        
        first_value(relationship_strength ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as relationship_strength,
        
        max(last_interaction_date) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
        ) as last_interaction_date,
        
        first_value(interaction_frequency ignore nulls) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end
            rows between unbounded preceding and unbounded following
        ) as interaction_frequency,
        
        -- Source system tracking
        string_agg(distinct source_system, ', ') over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
        ) as source_systems,
        
        count(*) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
        ) as source_record_count,
        
        min(created_date) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
        ) as created_date,
        
        max(last_modified_date) over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
        ) as last_modified_date,
        
        CURRENT_TIMESTAMP() as processed_at,
        
        -- Row number for deduplication
        row_number() over (
            partition by upper(REGEXP_REPLACE(counterparty_name, '[^A-Za-z0-9]', ''))
            order by case when source_system = 'CRM_VENDOR' then 1 else 2 end, created_date
        ) as rn

    from all_counterparties
),

-- Keep only the first record per counterparty name
unique_counterparties as (
    select * from consolidated_counterparties where rn = 1
),

-- Add enhanced counterparty profiling
enhanced_counterparties as (
    select
        *,
        
        -- Counterparty category for relationship management
        case 
            when counterparty_type in ('LEGAL_COUNSEL', 'AUDITOR') then 'PROFESSIONAL_SERVICES'
            when counterparty_type = 'SERVICE_PROVIDER' then 'ADVISORY_SERVICES'
            when counterparty_type in ('LENDER', 'CO_INVESTOR') then 'FINANCIAL_PARTNERS'
            when counterparty_type = 'VENDOR' then 'OPERATIONAL_VENDORS'
            else 'OTHER'
        end as counterparty_category,
        
        -- Relationship importance scoring
        case 
            when counterparty_type in ('LEGAL_COUNSEL', 'AUDITOR') and relationship_strength = 'STRONG' then 5
            when counterparty_type in ('LEGAL_COUNSEL', 'AUDITOR') then 4
            when counterparty_type = 'LENDER' and relationship_strength = 'STRONG' then 5
            when counterparty_type = 'LENDER' then 4
            when counterparty_type = 'CO_INVESTOR' and relationship_strength = 'STRONG' then 4
            when counterparty_type = 'CO_INVESTOR' then 3
            when counterparty_type = 'SERVICE_PROVIDER' and relationship_strength = 'STRONG' then 3
            when counterparty_type = 'SERVICE_PROVIDER' then 2
            when counterparty_type = 'VENDOR' then 1
            else 0
        end as relationship_importance_score,
        
        -- Engagement frequency assessment
        case 
            when interaction_frequency = 'DAILY' then 'HIGH_FREQUENCY'
            when interaction_frequency = 'WEEKLY' then 'MEDIUM_FREQUENCY'
            when interaction_frequency in ('MONTHLY', 'QUARTERLY') then 'LOW_FREQUENCY'
            when interaction_frequency in ('ANNUALLY', 'AS_NEEDED') then 'OCCASIONAL'
            else 'UNKNOWN'
        end as engagement_frequency,
        
        -- Relationship recency assessment
        case 
            when last_interaction_date is null then 'NO_RECENT_ACTIVITY'
            when DATE_DIFF(current_date(), last_interaction_date, MONTH) <= 3 then 'RECENT'
            when DATE_DIFF(current_date(), last_interaction_date, MONTH) <= 12 then 'MODERATE'
            when DATE_DIFF(current_date(), last_interaction_date, MONTH) <= 24 then 'STALE'
            else 'INACTIVE'
        end as relationship_recency,
        
        -- Data completeness assessment
        (
            case when counterparty_name is not null then 1 else 0 end +
            case when counterparty_type is not null then 1 else 0 end +
            case when primary_contact_name is not null then 1 else 0 end +
            case when primary_contact_email is not null then 1 else 0 end +
            case when country_code is not null then 1 else 0 end +
            case when relationship_status is not null then 1 else 0 end +
            case when last_interaction_date is not null then 1 else 0 end
        ) / 7.0 * 100 as completeness_score,
        
        -- Source system coverage assessment
        case 
            when source_record_count >= 3 then 'COMPREHENSIVE'
            when source_record_count = 2 then 'PARTIAL'
            when source_record_count = 1 then 'MINIMAL'
            else 'NO_COVERAGE'
        end as source_coverage

    from unique_counterparties
),

final as (
    select
        -- Canonical model format - exact column names and types expected by amos_core
        CAST(canonical_counterparty_id AS STRING) as id,
        CAST(counterparty_name AS STRING) as name,
        CAST(counterparty_type AS STRING) as type,
        CAST(country_code AS STRING) as country_code,
        CAST(created_date AS TIMESTAMP) as created_at,
        CAST(last_modified_date AS TIMESTAMP) as updated_at,
        
        -- Additional intermediate fields for analysis (not used by canonical model)
        primary_contact_name,
        primary_contact_email,
        counterparty_category,
        relationship_status,
        relationship_strength,
        last_interaction_date,
        
        -- Overall relationship value score
        (
            relationship_importance_score * 0.4 +
            case when relationship_recency = 'RECENT' then 3
                 when relationship_recency = 'MODERATE' then 2
                 when relationship_recency = 'STALE' then 1
                 else 0 end * 0.3 +
            case when engagement_frequency = 'HIGH_FREQUENCY' then 3
                 when engagement_frequency = 'MEDIUM_FREQUENCY' then 2
                 when engagement_frequency = 'LOW_FREQUENCY' then 1
                 else 0 end * 0.2 +
            case when completeness_score >= 80 then 2
                 when completeness_score >= 60 then 1
                 else 0 end * 0.1
        ) as overall_relationship_value,
        
        -- Relationship management priority
        case 
            when overall_relationship_value >= 4.0 and counterparty_category in ('PROFESSIONAL_SERVICES', 'FINANCIAL_PARTNERS') then 'HIGH_PRIORITY'
            when overall_relationship_value >= 3.0 and counterparty_category in ('PROFESSIONAL_SERVICES', 'ADVISORY_SERVICES', 'FINANCIAL_PARTNERS') then 'MEDIUM_PRIORITY'
            when overall_relationship_value >= 2.0 then 'LOW_PRIORITY'
            else 'MONITOR_ONLY'
        end as relationship_priority,
        
        -- Engagement strategy recommendation
        case 
            when relationship_priority = 'HIGH_PRIORITY' and relationship_recency in ('STALE', 'INACTIVE') then 'IMMEDIATE_OUTREACH'
            when relationship_priority = 'HIGH_PRIORITY' then 'REGULAR_ENGAGEMENT'
            when relationship_priority = 'MEDIUM_PRIORITY' and relationship_recency = 'RECENT' then 'MAINTAIN_CONTACT'
            when relationship_priority = 'MEDIUM_PRIORITY' then 'PERIODIC_OUTREACH'
            when relationship_priority = 'LOW_PRIORITY' then 'MONITOR_ACTIVITY'
            else 'NO_ACTION_REQUIRED'
        end as engagement_strategy,
        
        -- Overall data quality rating
        case 
            when completeness_score >= 85 and source_coverage in ('COMPREHENSIVE', 'PARTIAL') then 'HIGH'
            when completeness_score >= 65 and source_coverage != 'NO_COVERAGE' then 'MEDIUM'
            else 'LOW'
        end as data_quality_rating,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(canonical_counterparty_id, counterparty_name, counterparty_type, primary_contact_name, primary_contact_email, relationship_status, relationship_strength, last_interaction_date, last_modified_date)) as record_hash

    from enhanced_counterparties
    where canonical_counterparty_id is not null
)

select * from final