-- =====================================================
-- Bridge Transformation Macros for Column Mapping
-- =====================================================
-- 
-- These macros handle column name mismatches between
-- transformation layers in the AMOS data pipeline.
--
-- Version: 1.0
-- Last Updated: 2024-10-18
-- =====================================================

-- =====================================================
-- STAGING TO INTERMEDIATE BRIDGE TRANSFORMATIONS
-- =====================================================

{% macro bridge_company_staging_to_intermediate() %}
    -- Bridge transformation: staging CRM companies -> intermediate entities
    -- Handles column name mismatches and data type conversions
    
    -- Primary identifiers with cross-reference lookup
    x.canonical_company_id as id,
    c.company_id as crm_company_id,
    pm.pm_company_id,
    
    -- Company names (prioritize CRM, fallback to PM)
    COALESCE(c.company_name, pm.company_name) as name,
    
    -- PM investment summary fields
    pm.investment_count,
    pm.total_investment_amount,
    pm.first_investment_date,
    pm.latest_investment_date,
    c.legal_name as company_legal_name,
    
    -- Industry and classification
    c.industry_primary,
    c.industry_secondary,
    c.industry_sector,
    
    -- Geographic information
    c.standardized_country_code as country_code,
    c.state_province,
    c.city,
    
    -- Company characteristics
    c.founded_year,
    c.employee_count,
    c.company_size_category,
    c.revenue_range_text,
    c.revenue_midpoint_millions,
    
    -- Contact and web presence
    c.website_url as website,
    c.company_description as description,
    c.business_model,
    c.competitive_position,
    c.key_risks,
    
    -- Quality and scoring
    c.esg_score,
    c.data_quality_score as crm_data_quality_score,
    c.completeness_score as crm_completeness_score,
    c.overall_data_quality as crm_data_quality,
    
    -- Audit fields with type conversion
    CAST(c.created_date AS TIMESTAMP) as created_at,
    CAST(c.last_modified_date AS TIMESTAMP) as updated_at,
    
    -- Cross-reference metadata
    x.standardized_confidence as resolution_confidence,
    x.source_systems_count,
    x.data_quality_rating as xref_data_quality,
    x.primary_source_system
{% endmacro %}

{% macro bridge_fund_staging_to_intermediate() %}
    -- Bridge transformation: staging admin funds -> intermediate entities
    -- Maps fund administration fields to intermediate model structure
    
    -- Primary identifiers
    x.canonical_fund_id as id,
    f.fund_code as admin_fund_code,
    
    -- Fund identification
    f.fund_name as name,
    f.fund_legal_name,
    f.fund_type as type,
    
    -- Fund characteristics
    f.vintage_year as vintage,
    f.management_fee_rate as management_fee,
    f.hurdle_rate as hurdle,
    f.carried_interest_rate as carried_interest,
    f.target_size as target_commitment,
    f.final_size,
    
    -- Geographic and regulatory
    f.base_currency_code,
    
    -- Strategy and focus
    f.investment_strategy,
    f.sector_focus,
    f.fund_status,
    
    -- Audit fields
    CAST(f.created_date AS TIMESTAMP) as created_at,
    CAST(f.last_modified_date AS TIMESTAMP) as updated_at,
    
    -- Cross-reference data
    x.data_quality_rating as overall_data_quality
{% endmacro %}

{% macro bridge_investor_staging_to_intermediate() %}
    -- Bridge transformation: staging admin investors -> intermediate entities
    -- Maps investor administration fields to intermediate model structure
    
    -- Primary identifiers
    x.canonical_investor_id as id,
    i.investor_code as admin_investor_code,
    
    -- Investor identification
    i.investor_name as name,
    i.standardized_investor_type,
    
    -- Geographic information
    i.standardized_country_code,
    
    -- Investment characteristics
    i.investor_size_category,
    
    -- Audit fields
    CAST(i.created_date AS TIMESTAMP) as created_at,
    CAST(i.last_modified_date AS TIMESTAMP) as updated_at,
    
    -- Generated foreign keys
    null as investor_type_id
{% endmacro %}

{% macro bridge_counterparty_staging_to_intermediate() %}
    -- Bridge transformation: staging accounting counterparties -> intermediate entities
    -- Maps accounting system fields to intermediate model structure
    
    -- Primary identifiers (direct mapping for counterparties)
    cp.counterparty_id as id,
    
    -- Counterparty identification
    cp.counterparty_name as name,
    cp.counterparty_type as type,
    
    -- Geographic information
    cp.country_code,
    
    -- Contact information
    cp.primary_contact_name,
    cp.primary_contact_email,
    
    -- Relationship management
    cp.counterparty_category,
    cp.relationship_status,
    cp.relationship_strength,
    cp.last_interaction_date,
    cp.relationship_priority,
    cp.engagement_strategy,
    
    -- Quality assessment
    cp.data_quality_rating,
    
    -- Audit fields
    CAST(cp.created_timestamp AS TIMESTAMP) as created_at,
    CAST(cp.updated_timestamp AS TIMESTAMP) as updated_at
{% endmacro %}

-- =====================================================
-- INTERMEDIATE TO CANONICAL BRIDGE TRANSFORMATIONS
-- =====================================================

{% macro bridge_company_intermediate_to_canonical() %}
    -- Bridge transformation: intermediate company -> canonical company
    -- Ensures exact column mapping for canonical model contract
    
    -- Required canonical fields with exact data types
    CAST(canonical_company_id AS STRING) as id,
    CAST(company_name AS STRING) as name,
    CAST(website_url AS STRING) as website,
    CAST(company_description AS STRING) as description,
    
    -- Currency mapping with fallback logic
    CAST(COALESCE(country_code, 'USD') AS STRING) as currency,
    
    -- Generated foreign key for industry
    CAST(GENERATE_UUID() AS STRING) as industry_id,
    
    -- Timestamp fields
    CAST(created_date AS TIMESTAMP) as created_at,
    CAST(last_modified_date AS TIMESTAMP) as updated_at
{% endmacro %}

{% macro bridge_fund_intermediate_to_canonical() %}
    -- Bridge transformation: intermediate fund -> canonical fund
    -- Maps intermediate fund fields to canonical model requirements
    
    -- Required canonical fields
    CAST(canonical_fund_id AS STRING) as id,
    CAST(fund_name AS STRING) as name,
    CAST(fund_type AS STRING) as type,
    
    -- Numeric fields with proper casting
    CAST(vintage_year AS INT64) as vintage,
    CAST(management_fee_rate AS NUMERIC(7,4)) as management_fee,
    CAST(hurdle_rate AS NUMERIC(7,4)) as hurdle,
    CAST(carried_interest_rate AS NUMERIC(7,4)) as carried_interest,
    CAST(target_size AS NUMERIC(20,2)) as target_commitment,
    
    -- Geographic and regulatory
    CAST(jurisdiction AS STRING) as incorporated_in,
    CAST(base_currency AS STRING) as base_currency_code,
    
    -- Audit fields
    CAST(created_timestamp AS TIMESTAMP) as created_at,
    CAST(updated_timestamp AS TIMESTAMP) as updated_at
{% endmacro %}

{% macro bridge_investor_intermediate_to_canonical() %}
    -- Bridge transformation: intermediate investor -> canonical investor
    -- Maps intermediate investor fields to canonical model requirements
    
    -- Required canonical fields
    CAST(canonical_investor_id AS STRING) as id,
    CAST(investor_name AS STRING) as name,
    
    -- Generated foreign key for investor type
    CAST(GENERATE_UUID() AS STRING) as investor_type_id,
    
    -- Audit fields
    CAST(created_timestamp AS TIMESTAMP) as created_at,
    CAST(updated_timestamp AS TIMESTAMP) as updated_at
{% endmacro %}

{% macro bridge_counterparty_intermediate_to_canonical() %}
    -- Bridge transformation: intermediate counterparty -> canonical counterparty
    -- Maps intermediate counterparty fields to canonical model requirements
    
    -- Required canonical fields
    CAST(counterparty_id AS STRING) as id,
    CAST(counterparty_name AS STRING) as name,
    CAST(counterparty_type AS STRING) as type,
    
    -- Geographic information
    CAST(country_code AS STRING) as country_code,
    
    -- Audit fields
    CAST(created_timestamp AS TIMESTAMP) as created_at,
    CAST(updated_timestamp AS TIMESTAMP) as updated_at
{% endmacro %}

-- =====================================================
-- COLUMN ALIASING MACROS FOR EXPLICIT MAPPING
-- =====================================================

{% macro alias_staging_columns(entity_type) %}
    -- Generate column aliases for staging to intermediate mapping
    {% if entity_type == 'company' %}
        {{ amos_source_example.bridge_company_staging_to_intermediate() }}
    {% elif entity_type == 'fund' %}
        {{ amos_source_example.bridge_fund_staging_to_intermediate() }}
    {% elif entity_type == 'investor' %}
        {{ amos_source_example.bridge_investor_staging_to_intermediate() }}
    {% elif entity_type == 'counterparty' %}
        {{ amos_source_example.bridge_counterparty_staging_to_intermediate() }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown entity type: " ~ entity_type) }}
    {% endif %}
{% endmacro %}

{% macro alias_intermediate_columns(entity_type) %}
    -- Generate column aliases for intermediate to canonical mapping
    {% if entity_type == 'company' %}
        {{ amos_source_example.bridge_company_intermediate_to_canonical() }}
    {% elif entity_type == 'fund' %}
        {{ amos_source_example.bridge_fund_intermediate_to_canonical() }}
    {% elif entity_type == 'investor' %}
        {{ amos_source_example.bridge_investor_intermediate_to_canonical() }}
    {% elif entity_type == 'counterparty' %}
        {{ amos_source_example.bridge_counterparty_intermediate_to_canonical() }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown entity type: " ~ entity_type) }}
    {% endif %}
{% endmacro %}

-- =====================================================
-- FIELD MAPPING VALIDATION MACROS
-- =====================================================

{% macro validate_field_mapping(source_fields, target_fields) %}
    -- Validate that all required target fields are mapped from source
    {% set missing_fields = [] %}
    {% for target_field in target_fields %}
        {% if target_field not in source_fields %}
            {% do missing_fields.append(target_field) %}
        {% endif %}
    {% endfor %}
    
    {% if missing_fields %}
        {{ exceptions.raise_compiler_error("Missing field mappings: " ~ missing_fields | join(", ")) }}
    {% endif %}
{% endmacro %}

{% macro get_canonical_required_fields(entity_type) %}
    -- Return list of required fields for canonical models
    {% if entity_type == 'company' %}
        {% set required_fields = ['id', 'name'] %}
    {% elif entity_type == 'fund' %}
        {% set required_fields = ['id', 'name', 'base_currency_code'] %}
    {% elif entity_type == 'investor' %}
        {% set required_fields = ['id', 'name'] %}
    {% elif entity_type == 'counterparty' %}
        {% set required_fields = ['id', 'name'] %}
    {% else %}
        {% set required_fields = [] %}
    {% endif %}
    
    {{ return(required_fields) }}
{% endmacro %}

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================

/*
-- Example 1: Use in intermediate model
SELECT
    {{ alias_staging_columns('company') }}
FROM {{ ref('stg_crm_companies') }} c
LEFT JOIN {{ ref('stg_ref_xref_companies') }} x 
    ON x.crm_company_id = c.company_id

-- Example 2: Use in canonical model  
SELECT
    {{ alias_intermediate_columns('company') }}
FROM {{ ref('int_entities_company') }}
WHERE id IS NOT NULL AND name IS NOT NULL

-- Example 3: Validate field mapping
{{ validate_field_mapping(
    ['canonical_company_id', 'company_name', 'website_url'], 
    get_canonical_required_fields('company')
) }}
*/