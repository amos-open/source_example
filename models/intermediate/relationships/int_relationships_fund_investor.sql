{{
  config(
    materialized='table',
    tags=['intermediate', 'relationships']
  )
}}

/*
  Intermediate model for fund-investor relationship preparation
  
  This model prepares fund-investor commitment relationships from fund administration data,
  combining commitment information with transaction history to create comprehensive
  relationship records for canonical consumption.
  
  Data sources:
  - Fund administration capital calls (for commitment inference)
  - Fund administration distributions (for relationship validation)
  - Fund cross-reference mappings for entity resolution
  - Investor staging data for investor details
  
  Business logic:
  - Infer commitment relationships from capital call patterns
  - Calculate commitment amounts and deployment metrics
  - Assess relationship quality and performance
  - Prepare relationship data for canonical bridge tables
*/

with admin_capital_calls as (
    select * from {{ ref('stg_admin_capital_calls') }}
),

admin_distributions as (
    select * from {{ ref('stg_admin_distributions') }}
),

admin_investors as (
    select * from {{ ref('stg_admin_investors') }}
),

fund_xref as (
    select * from {{ ref('stg_ref_xref_funds') }}
    where recommended_for_resolution = true
),

-- Aggregate capital call data by fund-investor relationship
fund_investor_capital_calls as (
    select
        acc.fund_code,
        acc.investor_code,
        
        -- Commitment metrics inferred from capital calls
        count(*) as total_capital_calls,
        sum(acc.call_amount) as total_called_amount,
        sum(acc.payment_amount) as total_paid_amount,
        min(acc.call_date) as first_call_date,
        max(acc.call_date) as latest_call_date,
        
        -- Commitment amount inference (using maximum commitment amount seen)
        max(acc.commitment_amount) as inferred_commitment_amount,
        max(acc.commitment_currency) as commitment_currency,
        
        -- Payment performance metrics
        avg(acc.payment_completion_percentage) as avg_payment_completion_rate,
        sum(case when acc.standardized_payment_status = 'PAID' then 1 else 0 end) as paid_calls_count,
        sum(case when acc.standardized_payment_status = 'OVERDUE' then 1 else 0 end) as overdue_calls_count,
        avg(acc.days_late_early) as avg_payment_delay_days,
        
        -- Call composition analysis
        sum(acc.management_fee_amount) as total_management_fees_called,
        sum(acc.investment_amount) as total_investment_amount_called,
        sum(acc.expense_amount) as total_expenses_called

    from admin_capital_calls acc
    where acc.fund_code is not null 
        and acc.investor_code is not null
    group by acc.fund_code, acc.investor_code
),

-- Aggregate distribution data by fund-investor relationship
fund_investor_distributions as (
    select
        ad.fund_code,
        ad.investor_code,
        
        -- Distribution metrics
        count(*) as total_distributions,
        sum(ad.distribution_amount) as total_distributed_amount,
        sum(ad.net_distribution_amount) as total_net_distributed_amount,
        min(ad.distribution_date) as first_distribution_date,
        max(ad.distribution_date) as latest_distribution_date,
        
        -- Distribution type analysis
        sum(case when ad.standardized_distribution_type = 'RETURN_OF_CAPITAL' then ad.distribution_amount else 0 end) as return_of_capital_amount,
        sum(case when ad.standardized_distribution_type = 'CAPITAL_GAIN' then ad.distribution_amount else 0 end) as capital_gain_amount,
        sum(case when ad.standardized_distribution_type = 'DIVIDEND_INCOME' then ad.distribution_amount else 0 end) as dividend_income_amount,
        sum(case when ad.standardized_distribution_type = 'CARRIED_INTEREST' then ad.distribution_amount else 0 end) as carried_interest_amount,
        
        -- Tax efficiency metrics
        avg(ad.withholding_tax_rate) as avg_withholding_tax_rate,
        sum(ad.withholding_tax_amount) as total_withholding_tax_amount

    from admin_distributions ad
    where ad.fund_code is not null 
        and ad.investor_code is not null
    group by ad.fund_code, ad.investor_code
),

-- Prepare fund-investor relationships with entity resolution
fund_investor_relationships as (
    select
        -- Generate relationship identifier
        'REL-FI-' || coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || ficc.fund_code) || '-' || ficc.investor_code as relationship_id,
        'FUND_INVESTOR' as relationship_type,
        'FUND_ADMIN_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || ficc.fund_code) as canonical_fund_id,
        ficc.fund_code as source_fund_id,
        ficc.investor_code as source_investor_id,
        
        -- Fund and investor details
        fx.canonical_fund_name as fund_name,
        ai.investor_name,
        ai.standardized_investor_type as investor_type,
        ai.investor_size_category,
        ai.geographic_region as investor_region,
        
        -- Commitment information
        ficc.inferred_commitment_amount as commitment_amount,
        ficc.commitment_currency,
        
        -- Capital call metrics
        ficc.total_capital_calls,
        ficc.total_called_amount,
        ficc.total_paid_amount,
        ficc.first_call_date,
        ficc.latest_call_date,
        
        -- Distribution metrics
        coalesce(fid.total_distributions, 0) as total_distributions,
        coalesce(fid.total_distributed_amount, 0) as total_distributed_amount,
        coalesce(fid.total_net_distributed_amount, 0) as total_net_distributed_amount,
        fid.first_distribution_date,
        fid.latest_distribution_date,
        
        -- Performance metrics
        ficc.avg_payment_completion_rate,
        ficc.paid_calls_count,
        ficc.overdue_calls_count,
        ficc.avg_payment_delay_days,
        
        -- Distribution composition
        coalesce(fid.return_of_capital_amount, 0) as return_of_capital_amount,
        coalesce(fid.capital_gain_amount, 0) as capital_gain_amount,
        coalesce(fid.dividend_income_amount, 0) as dividend_income_amount,
        coalesce(fid.carried_interest_amount, 0) as carried_interest_amount,
        
        -- Tax metrics
        fid.avg_withholding_tax_rate,
        coalesce(fid.total_withholding_tax_amount, 0) as total_withholding_tax_amount,
        
        -- Call composition
        ficc.total_management_fees_called,
        ficc.total_investment_amount_called,
        ficc.total_expenses_called,
        
        -- Investor characteristics
        ai.compliance_status,
        ai.investment_capacity,
        ai.risk_tolerance,
        ai.has_esg_requirements,
        ai.fundraising_status,
        
        -- Relationship timeline
        ficc.first_call_date as relationship_start_date,
        greatest(
            coalesce(ficc.latest_call_date, '1900-01-01'::date),
            coalesce(fid.latest_distribution_date, '1900-01-01'::date)
        ) as latest_activity_date,
        
        current_timestamp() as processed_at

    from fund_investor_capital_calls ficc
    left join fund_investor_distributions fid 
        on ficc.fund_code = fid.fund_code 
        and ficc.investor_code = fid.investor_code
    left join fund_xref fx on ficc.fund_code = fx.admin_fund_code
    left join admin_investors ai on ficc.investor_code = ai.investor_code
),

-- Add enhanced relationship analytics
enhanced_relationships as (
    select
        *,
        
        -- Calculate deployment and return metrics
        case 
            when commitment_amount is not null and commitment_amount > 0 then
                (total_called_amount / commitment_amount) * 100
            else null
        end as capital_deployment_percentage,
        
        case 
            when total_called_amount is not null and total_called_amount > 0 then
                (total_distributed_amount / total_called_amount) * 100
            else null
        end as capital_returned_percentage,
        
        case 
            when total_called_amount is not null and total_called_amount > 0 then
                total_distributed_amount / total_called_amount
            else null
        end as dpi_ratio,
        
        -- Payment reliability assessment
        case 
            when total_capital_calls > 0 then
                (paid_calls_count::float / total_capital_calls) * 100
            else null
        end as payment_reliability_percentage,
        
        case 
            when avg_payment_delay_days <= 0 then 'EARLY_PAYER'
            when avg_payment_delay_days <= 5 then 'ON_TIME_PAYER'
            when avg_payment_delay_days <= 15 then 'SLIGHTLY_LATE_PAYER'
            when avg_payment_delay_days <= 30 then 'LATE_PAYER'
            else 'CHRONIC_LATE_PAYER'
        end as payment_behavior_category,
        
        -- Relationship maturity assessment
        case 
            when relationship_start_date is not null then
                datediff('year', relationship_start_date, current_date())
            else null
        end as relationship_age_years,
        
        case 
            when relationship_age_years >= 7 then 'MATURE_RELATIONSHIP'
            when relationship_age_years >= 4 then 'ESTABLISHED_RELATIONSHIP'
            when relationship_age_years >= 2 then 'DEVELOPING_RELATIONSHIP'
            when relationship_age_years >= 1 then 'NEW_RELATIONSHIP'
            else 'VERY_NEW_RELATIONSHIP'
        end as relationship_maturity,
        
        -- Activity recency assessment
        case 
            when latest_activity_date is not null then
                datediff('month', latest_activity_date, current_date())
            else null
        end as months_since_last_activity,
        
        case 
            when months_since_last_activity <= 3 then 'ACTIVE'
            when months_since_last_activity <= 12 then 'RECENT_ACTIVITY'
            when months_since_last_activity <= 24 then 'DORMANT'
            else 'INACTIVE'
        end as activity_status,
        
        -- Commitment size categorization
        case 
            when commitment_amount >= 100000000 then 'MEGA_COMMITMENT'     -- $100M+
            when commitment_amount >= 25000000 then 'LARGE_COMMITMENT'     -- $25M-$100M
            when commitment_amount >= 5000000 then 'MEDIUM_COMMITMENT'     -- $5M-$25M
            when commitment_amount >= 1000000 then 'SMALL_COMMITMENT'      -- $1M-$5M
            when commitment_amount > 0 then 'MICRO_COMMITMENT'             -- <$1M
            else 'UNKNOWN_COMMITMENT'
        end as commitment_size_category,
        
        -- Distribution profile analysis
        case 
            when total_distributed_amount > 0 then
                case 
                    when capital_gain_amount / total_distributed_amount >= 0.7 then 'CAPITAL_GAINS_FOCUSED'
                    when return_of_capital_amount / total_distributed_amount >= 0.7 then 'CAPITAL_RETURN_FOCUSED'
                    when dividend_income_amount / total_distributed_amount >= 0.5 then 'INCOME_FOCUSED'
                    else 'MIXED_DISTRIBUTIONS'
                end
            else 'NO_DISTRIBUTIONS'
        end as distribution_profile,
        
        -- Tax efficiency assessment
        case 
            when avg_withholding_tax_rate is null or avg_withholding_tax_rate = 0 then 'TAX_EFFICIENT'
            when avg_withholding_tax_rate <= 5 then 'LOW_TAX_BURDEN'
            when avg_withholding_tax_rate <= 15 then 'MODERATE_TAX_BURDEN'
            else 'HIGH_TAX_BURDEN'
        end as tax_efficiency,
        
        -- Investor value assessment
        case 
            when investor_size_category = 'LARGE_INSTITUTIONAL' 
                and commitment_size_category in ('MEGA_COMMITMENT', 'LARGE_COMMITMENT')
                and payment_behavior_category in ('EARLY_PAYER', 'ON_TIME_PAYER') then 'PREMIUM_INVESTOR'
            when investor_size_category in ('LARGE_INSTITUTIONAL', 'MEDIUM_INSTITUTIONAL')
                and commitment_size_category in ('LARGE_COMMITMENT', 'MEDIUM_COMMITMENT')
                and payment_reliability_percentage >= 95 then 'HIGH_VALUE_INVESTOR'
            when commitment_size_category in ('MEDIUM_COMMITMENT', 'SMALL_COMMITMENT')
                and payment_reliability_percentage >= 90 then 'SOLID_INVESTOR'
            when payment_reliability_percentage >= 80 then 'STANDARD_INVESTOR'
            else 'CHALLENGING_INVESTOR'
        end as investor_value_tier

    from fund_investor_relationships
),

-- Final relationship preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall relationship quality score (0-100)
        (
            case when commitment_size_category = 'MEGA_COMMITMENT' then 25
                 when commitment_size_category = 'LARGE_COMMITMENT' then 20
                 when commitment_size_category = 'MEDIUM_COMMITMENT' then 15
                 when commitment_size_category = 'SMALL_COMMITMENT' then 10
                 when commitment_size_category = 'MICRO_COMMITMENT' then 5
                 else 0 end +
            case when payment_behavior_category = 'EARLY_PAYER' then 20
                 when payment_behavior_category = 'ON_TIME_PAYER' then 18
                 when payment_behavior_category = 'SLIGHTLY_LATE_PAYER' then 12
                 when payment_behavior_category = 'LATE_PAYER' then 6
                 else 0 end +
            case when investor_size_category = 'LARGE_INSTITUTIONAL' then 15
                 when investor_size_category = 'MEDIUM_INSTITUTIONAL' then 12
                 when investor_size_category = 'PRIVATE_WEALTH' then 8
                 when investor_size_category = 'CORPORATE' then 5
                 else 0 end +
            case when compliance_status = 'FULLY_COMPLIANT' then 15
                 when compliance_status = 'PARTIALLY_COMPLIANT' then 10
                 else 0 end +
            case when relationship_maturity in ('MATURE_RELATIONSHIP', 'ESTABLISHED_RELATIONSHIP') then 10
                 when relationship_maturity = 'DEVELOPING_RELATIONSHIP' then 7
                 when relationship_maturity = 'NEW_RELATIONSHIP' then 5
                 else 2 end +
            case when activity_status = 'ACTIVE' then 10
                 when activity_status = 'RECENT_ACTIVITY' then 7
                 when activity_status = 'DORMANT' then 3
                 else 0 end +
            case when tax_efficiency in ('TAX_EFFICIENT', 'LOW_TAX_BURDEN') then 5
                 when tax_efficiency = 'MODERATE_TAX_BURDEN' then 3
                 else 0 end
        ) as relationship_quality_score,
        
        -- Fundraising priority for future funds
        case 
            when investor_value_tier = 'PREMIUM_INVESTOR' and activity_status = 'ACTIVE' then 'TOP_PRIORITY'
            when investor_value_tier in ('PREMIUM_INVESTOR', 'HIGH_VALUE_INVESTOR') then 'HIGH_PRIORITY'
            when investor_value_tier = 'SOLID_INVESTOR' and relationship_maturity in ('MATURE_RELATIONSHIP', 'ESTABLISHED_RELATIONSHIP') then 'MEDIUM_PRIORITY'
            when investor_value_tier in ('SOLID_INVESTOR', 'STANDARD_INVESTOR') then 'LOW_PRIORITY'
            else 'EXCLUDE'
        end as fundraising_priority,
        
        -- Relationship management priority
        case 
            when investor_value_tier = 'CHALLENGING_INVESTOR' and commitment_size_category in ('MEGA_COMMITMENT', 'LARGE_COMMITMENT') then 'HIGH_ATTENTION'
            when payment_behavior_category in ('LATE_PAYER', 'CHRONIC_LATE_PAYER') then 'HIGH_ATTENTION'
            when investor_value_tier = 'PREMIUM_INVESTOR' then 'WHITE_GLOVE_SERVICE'
            when investor_value_tier = 'HIGH_VALUE_INVESTOR' then 'PRIORITY_SERVICE'
            else 'STANDARD_SERVICE'
        end as service_level,
        
        -- Re-up potential assessment
        case 
            when investor_value_tier in ('PREMIUM_INVESTOR', 'HIGH_VALUE_INVESTOR') 
                and dpi_ratio >= 1.0 
                and activity_status = 'ACTIVE' then 'HIGHLY_LIKELY'
            when investor_value_tier in ('HIGH_VALUE_INVESTOR', 'SOLID_INVESTOR') 
                and dpi_ratio >= 0.5 then 'LIKELY'
            when investor_value_tier = 'SOLID_INVESTOR' 
                and payment_reliability_percentage >= 95 then 'POSSIBLE'
            when investor_value_tier = 'STANDARD_INVESTOR' then 'UNCERTAIN'
            else 'UNLIKELY'
        end as re_up_potential,
        
        -- Data quality assessment
        case 
            when commitment_amount is not null 
                and total_capital_calls > 0 
                and payment_reliability_percentage is not null
                and relationship_start_date is not null then 'HIGH_QUALITY'
            when commitment_amount is not null 
                and total_capital_calls > 0 then 'MEDIUM_QUALITY'
            when total_capital_calls > 0 then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as data_quality_rating,
        
        -- Record hash for change detection
        hash(
            relationship_id,
            canonical_fund_id,
            source_investor_id,
            commitment_amount,
            total_called_amount,
            total_distributed_amount,
            total_capital_calls,
            total_distributions,
            latest_activity_date
        ) as record_hash

    from enhanced_relationships
)

select * from final