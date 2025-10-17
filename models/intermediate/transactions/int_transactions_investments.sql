{{
  config(
    materialized='table',
    tags=['intermediate', 'transactions']
  )
}}

/*
  Intermediate model for investment transaction preparation
  
  This model standardizes investment transactions from the portfolio management system,
  applying consistent categorization, currency conversion logic, and investment processing
  to prepare data for canonical transaction models.
  
  Data sources:
  - Portfolio management investments (primary)
  - Company cross-reference mappings for entity resolution
  - Fund cross-reference mappings for entity resolution
  - Currency exchange rates for conversion
  
  Business logic:
  - Standardize investment categorization and terms
  - Apply currency conversion to base currency
  - Calculate derived metrics and investment analytics
  - Prepare investment data for canonical consumption
*/

with pm_investments as (
    select * from {{ ref('stg_pm_investments') }}
),

company_xref as (
    select * from {{ ref('stg_ref_xref_companies') }}
    where data_quality_rating in ('HIGH_QUALITY', 'MEDIUM_QUALITY')
),

fund_xref as (
    select * from {{ ref('stg_ref_xref_funds') }}
    where recommended_for_resolution = true
),

fx_rates as (
    select * from {{ ref('stg_ref_fx_rates') }}
),

-- Get latest exchange rates for currency conversion
latest_fx_rates as (
    select
        from_currency,
        to_currency,
        exchange_rate,
        rate_date,
        row_number() over (
            partition by from_currency, to_currency 
            order by rate_date desc
        ) as rn
    from fx_rates
    where rate_date <= current_date()
),

current_fx_rates as (
    select * from latest_fx_rates where rn = 1
),

-- Prepare investment transactions with entity resolution
investment_transactions as (
    select
        pi.investment_id as transaction_id,
        'INVESTMENT' as transaction_type,
        'PORTFOLIO_MGMT_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(cx.canonical_company_id, 'COMP-UNKNOWN-' || pi.company_id) as canonical_company_id,
        coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || pi.fund_id) as canonical_fund_id,
        pi.company_id as source_company_id,
        pi.fund_id as source_fund_id,
        
        -- Company and fund names
        pi.company_name,
        pi.fund_name,
        
        -- Transaction date
        pi.investment_date as transaction_date,
        
        -- Investment amounts in original currency
        pi.initial_investment_amount as initial_amount,
        pi.validated_initial_currency as initial_currency,
        pi.total_invested_amount as total_amount,
        pi.validated_total_currency as total_currency,
        pi.follow_on_investment_amount as follow_on_amount,
        
        -- Investment terms and structure
        pi.standardized_investment_type as investment_type,
        pi.standardized_investment_stage as investment_stage,
        pi.standardized_sector as sector,
        pi.standardized_geography as geography,
        
        -- Ownership and governance
        pi.ownership_percentage,
        pi.ownership_category,
        pi.board_seats,
        pi.governance_influence,
        
        -- Legal terms
        pi.liquidation_preference_type,
        pi.standardized_anti_dilution as anti_dilution_protection,
        pi.has_drag_along_rights,
        pi.has_tag_along_rights,
        
        -- Strategic information
        pi.investment_thesis,
        pi.key_risks,
        pi.standardized_exit_strategy as exit_strategy,
        pi.target_exit_date,
        
        -- Investment analytics
        pi.investment_age_months,
        pi.investment_age_years,
        pi.target_holding_period_years,
        pi.investment_size_category,
        pi.investment_profile,
        pi.risk_assessment,
        
        -- Audit and metadata
        pi.created_date,
        pi.last_modified_date,
        pi.completeness_score,
        current_timestamp() as processed_at

    from pm_investments pi
    left join company_xref cx on pi.company_id = cx.pm_company_id
    left join fund_xref fx on pi.fund_id = fx.pm_fund_id
),

-- Apply currency conversion to USD base currency
currency_converted_transactions as (
    select
        *,
        
        -- Convert initial investment amount to USD
        case 
            when initial_currency = 'USD' then initial_amount
            when fx_initial.exchange_rate is not null then initial_amount * fx_initial.exchange_rate
            else initial_amount  -- Keep original if no rate available
        end as initial_amount_usd,
        
        -- Convert total investment amount to USD
        case 
            when total_currency = 'USD' then total_amount
            when fx_total.exchange_rate is not null then total_amount * fx_total.exchange_rate
            else total_amount  -- Keep original if no rate available
        end as total_amount_usd,
        
        -- Convert follow-on amount to USD
        case 
            when total_currency = 'USD' then follow_on_amount
            when fx_total.exchange_rate is not null then follow_on_amount * fx_total.exchange_rate
            else follow_on_amount
        end as follow_on_amount_usd,
        
        -- Currency conversion metadata
        fx_initial.exchange_rate as initial_fx_rate,
        fx_total.exchange_rate as total_fx_rate,
        case 
            when (initial_currency != 'USD' and fx_initial.exchange_rate is not null) 
                or (total_currency != 'USD' and fx_total.exchange_rate is not null) then true
            else false
        end as currency_converted

    from investment_transactions it
    left join current_fx_rates fx_initial 
        on it.initial_currency = fx_initial.from_currency 
        and fx_initial.to_currency = 'USD'
    left join current_fx_rates fx_total 
        on it.total_currency = fx_total.from_currency 
        and fx_total.to_currency = 'USD'
),

-- Add enhanced transaction categorization and analytics
enhanced_transactions as (
    select
        *,
        
        -- Transaction size categorization (in USD, based on total investment)
        case 
            when total_amount_usd >= 100000000 then 'MEGA_DEAL'
            when total_amount_usd >= 50000000 then 'LARGE_DEAL'
            when total_amount_usd >= 10000000 then 'MEDIUM_DEAL'
            when total_amount_usd >= 1000000 then 'SMALL_DEAL'
            when total_amount_usd > 0 then 'MICRO_DEAL'
            else 'UNKNOWN'
        end as deal_size_category,
        
        -- Investment strategy classification
        case 
            when investment_stage = 'BUYOUT' and ownership_category = 'MAJORITY' then 'CONTROL_BUYOUT'
            when investment_stage = 'BUYOUT' and ownership_category in ('SIGNIFICANT_MINORITY', 'MINORITY') then 'MINORITY_BUYOUT'
            when investment_stage in ('GROWTH', 'LATE_STAGE') and ownership_category in ('SIGNIFICANT_MINORITY', 'MINORITY') then 'GROWTH_EQUITY'
            when investment_stage in ('SEED', 'EARLY_STAGE') then 'VENTURE_CAPITAL'
            when investment_stage = 'MEZZANINE' then 'MEZZANINE_FINANCING'
            when investment_stage = 'DISTRESSED' then 'DISTRESSED_INVESTMENT'
            else 'OTHER_STRATEGY'
        end as investment_strategy_classification,
        
        -- Follow-on investment analysis
        case 
            when follow_on_amount_usd is null or follow_on_amount_usd <= 0 then 'INITIAL_ONLY'
            when follow_on_amount_usd > 0 and initial_amount_usd > 0 then
                case 
                    when follow_on_amount_usd / initial_amount_usd >= 1.0 then 'SIGNIFICANT_FOLLOW_ON'
                    when follow_on_amount_usd / initial_amount_usd >= 0.5 then 'MODERATE_FOLLOW_ON'
                    else 'MINOR_FOLLOW_ON'
                end
            else 'FOLLOW_ON_ONLY'
        end as follow_on_pattern,
        
        -- Follow-on ratio calculation
        case 
            when initial_amount_usd > 0 and follow_on_amount_usd > 0 then
                follow_on_amount_usd / initial_amount_usd
            else null
        end as follow_on_ratio,
        
        -- Governance control assessment
        case 
            when ownership_category = 'MAJORITY' and governance_influence = 'STRONG_GOVERNANCE' then 'FULL_CONTROL'
            when ownership_category in ('SIGNIFICANT_MINORITY', 'MAJORITY') and governance_influence in ('STRONG_GOVERNANCE', 'BOARD_REPRESENTATION') then 'OPERATIONAL_CONTROL'
            when ownership_category in ('MINORITY', 'SIGNIFICANT_MINORITY') and governance_influence = 'BOARD_REPRESENTATION' then 'BOARD_INFLUENCE'
            when ownership_category in ('MINORITY', 'SMALL_STAKE') then 'PASSIVE_INVESTMENT'
            else 'UNKNOWN_CONTROL'
        end as control_classification,
        
        -- Investment protection assessment
        case 
            when liquidation_preference_type in ('PARTICIPATING', 'SIMPLE_PREFERRED') 
                and anti_dilution_protection in ('WEIGHTED_AVERAGE_BROAD', 'WEIGHTED_AVERAGE_NARROW', 'FULL_RATCHET')
                and has_drag_along_rights = true and has_tag_along_rights = true then 'HIGHLY_PROTECTED'
            when liquidation_preference_type in ('PARTICIPATING', 'SIMPLE_PREFERRED') 
                and anti_dilution_protection in ('WEIGHTED_AVERAGE_BROAD', 'WEIGHTED_AVERAGE_NARROW') then 'WELL_PROTECTED'
            when liquidation_preference_type in ('PARTICIPATING', 'SIMPLE_PREFERRED') 
                or anti_dilution_protection != 'NONE' then 'MODERATELY_PROTECTED'
            else 'MINIMALLY_PROTECTED'
        end as protection_level,
        
        -- Sector risk assessment
        case 
            when sector = 'TECHNOLOGY' and investment_stage in ('SEED', 'EARLY_STAGE') then 'HIGH_RISK_HIGH_REWARD'
            when sector = 'TECHNOLOGY' and investment_stage in ('GROWTH', 'LATE_STAGE') then 'MODERATE_RISK_HIGH_GROWTH'
            when sector = 'HEALTHCARE' then 'REGULATED_SECTOR_RISK'
            when sector = 'FINANCIAL_SERVICES' then 'CYCLICAL_RISK'
            when sector in ('CONSUMER', 'INDUSTRIALS') then 'MARKET_DEPENDENT_RISK'
            when sector = 'ENERGY' then 'COMMODITY_RISK'
            else 'SECTOR_UNKNOWN_RISK'
        end as sector_risk_profile,
        
        -- Geographic risk assessment
        case 
            when geography = 'NORTH_AMERICA' then 'LOW_GEOGRAPHIC_RISK'
            when geography = 'EUROPE' then 'LOW_MODERATE_GEOGRAPHIC_RISK'
            when geography = 'ASIA_PACIFIC' then 'MODERATE_GEOGRAPHIC_RISK'
            when geography = 'LATIN_AMERICA' then 'MODERATE_HIGH_GEOGRAPHIC_RISK'
            else 'UNKNOWN_GEOGRAPHIC_RISK'
        end as geographic_risk_profile,
        
        -- Investment maturity assessment
        case 
            when investment_age_years >= 7 then 'MATURE_INVESTMENT'
            when investment_age_years >= 4 then 'SEASONED_INVESTMENT'
            when investment_age_years >= 2 then 'DEVELOPING_INVESTMENT'
            when investment_age_years >= 1 then 'EARLY_INVESTMENT'
            else 'NEW_INVESTMENT'
        end as investment_maturity,
        
        -- Exit timeline assessment
        case 
            when target_exit_date is not null then
                case 
                    when datediff('year', current_date(), target_exit_date) <= 1 then 'NEAR_TERM_EXIT'
                    when datediff('year', current_date(), target_exit_date) <= 3 then 'MEDIUM_TERM_EXIT'
                    when datediff('year', current_date(), target_exit_date) <= 5 then 'LONG_TERM_EXIT'
                    else 'EXTENDED_HOLD'
                end
            else 'NO_TARGET_EXIT'
        end as exit_timeline,
        
        -- Days since investment
        case 
            when transaction_date is not null then
                datediff('day', transaction_date, current_date())
            else null
        end as days_since_investment,
        
        -- Transaction recency classification
        case 
            when days_since_investment <= 90 then 'RECENT'
            when days_since_investment <= 365 then 'CURRENT_YEAR'
            when days_since_investment <= 1095 then 'RECENT_VINTAGE'  -- 3 years
            when days_since_investment <= 2555 then 'MATURE_VINTAGE'  -- 7 years
            else 'OLD_VINTAGE'
        end as investment_recency

    from currency_converted_transactions
),

-- Final transaction preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall investment attractiveness score
        (
            case when deal_size_category in ('MEGA_DEAL', 'LARGE_DEAL') then 3
                 when deal_size_category = 'MEDIUM_DEAL' then 2
                 when deal_size_category = 'SMALL_DEAL' then 1
                 else 0 end * 0.2 +
            case when control_classification in ('FULL_CONTROL', 'OPERATIONAL_CONTROL') then 3
                 when control_classification = 'BOARD_INFLUENCE' then 2
                 when control_classification = 'PASSIVE_INVESTMENT' then 1
                 else 0 end * 0.25 +
            case when protection_level = 'HIGHLY_PROTECTED' then 3
                 when protection_level = 'WELL_PROTECTED' then 2
                 when protection_level = 'MODERATELY_PROTECTED' then 1
                 else 0 end * 0.2 +
            case when sector in ('TECHNOLOGY', 'HEALTHCARE', 'FINANCIAL_SERVICES') then 2
                 else 1 end * 0.15 +
            case when geographic_risk_profile in ('LOW_GEOGRAPHIC_RISK', 'LOW_MODERATE_GEOGRAPHIC_RISK') then 2
                 else 1 end * 0.1 +
            case when completeness_score >= 90 then 2
                 when completeness_score >= 70 then 1
                 else 0 end * 0.1
        ) as investment_attractiveness_score,
        
        -- Risk-adjusted return potential
        case 
            when investment_strategy_classification = 'VENTURE_CAPITAL' and sector = 'TECHNOLOGY' then 'HIGH_RISK_HIGH_RETURN'
            when investment_strategy_classification = 'GROWTH_EQUITY' and sector in ('TECHNOLOGY', 'HEALTHCARE') then 'MODERATE_RISK_HIGH_RETURN'
            when investment_strategy_classification = 'CONTROL_BUYOUT' then 'MODERATE_RISK_MODERATE_RETURN'
            when investment_strategy_classification = 'MEZZANINE_FINANCING' then 'LOW_RISK_MODERATE_RETURN'
            when investment_strategy_classification = 'DISTRESSED_INVESTMENT' then 'HIGH_RISK_VARIABLE_RETURN'
            else 'UNKNOWN_RISK_RETURN'
        end as risk_return_profile,
        
        -- Portfolio monitoring priority
        case 
            when investment_maturity in ('MATURE_INVESTMENT', 'SEASONED_INVESTMENT') 
                and exit_timeline in ('NEAR_TERM_EXIT', 'MEDIUM_TERM_EXIT') then 'HIGH_PRIORITY'
            when control_classification in ('FULL_CONTROL', 'OPERATIONAL_CONTROL') then 'HIGH_PRIORITY'
            when deal_size_category in ('MEGA_DEAL', 'LARGE_DEAL') then 'MEDIUM_PRIORITY'
            when investment_maturity = 'NEW_INVESTMENT' then 'MEDIUM_PRIORITY'
            else 'LOW_PRIORITY'
        end as monitoring_priority,
        
        -- Data quality flags
        case 
            when currency_converted and (initial_fx_rate is null or total_fx_rate is null) then 'FX_RATE_MISSING'
            when completeness_score < 70 then 'INCOMPLETE_DATA'
            when investment_thesis is null or key_risks is null then 'MISSING_STRATEGIC_INFO'
            when ownership_percentage is null then 'MISSING_OWNERSHIP_DATA'
            when exit_strategy is null then 'NO_EXIT_STRATEGY'
            else 'NO_ISSUES'
        end as data_quality_flag,
        
        -- Investment lifecycle status
        case 
            when exit_timeline = 'NEAR_TERM_EXIT' then 'EXIT_PREPARATION'
            when investment_maturity in ('MATURE_INVESTMENT', 'SEASONED_INVESTMENT') then 'VALUE_CREATION'
            when investment_maturity = 'DEVELOPING_INVESTMENT' then 'GROWTH_PHASE'
            when investment_maturity in ('EARLY_INVESTMENT', 'NEW_INVESTMENT') then 'INTEGRATION_PHASE'
            else 'UNKNOWN_PHASE'
        end as lifecycle_status,
        
        -- Record hash for change detection
        hash(
            transaction_id,
            canonical_company_id,
            canonical_fund_id,
            transaction_date,
            total_amount_usd,
            investment_type,
            investment_stage,
            ownership_percentage,
            exit_strategy,
            last_modified_date
        ) as record_hash

    from enhanced_transactions
)

select * from final