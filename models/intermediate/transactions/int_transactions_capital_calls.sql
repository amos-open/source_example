{{
  config(
    materialized='table',
    tags=['intermediate', 'transactions']
  )
}}

/*
  Intermediate model for capital call transaction preparation
  
  This model standardizes capital call transactions from the fund administration system,
  applying consistent categorization, currency conversion logic, and transaction processing
  to prepare data for canonical transaction models.
  
  Data sources:
  - Fund administration capital calls (primary)
  - Fund cross-reference mappings for entity resolution
  - Investor cross-reference mappings for entity resolution
  - Currency exchange rates for conversion
  
  Business logic:
  - Standardize transaction categorization and amounts
  - Apply currency conversion to base currency
  - Calculate derived metrics and performance indicators
  - Prepare transaction data for canonical consumption
*/

with admin_capital_calls as (
    select * from {{ ref('stg_admin_capital_calls') }}
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

-- Prepare capital call transactions with entity resolution
capital_call_transactions as (
    select
        cc.capital_call_id as transaction_id,
        'CAPITAL_CALL' as transaction_type,
        'FUND_ADMIN_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || cc.fund_code) as canonical_fund_id,
        cc.fund_code as source_fund_id,
        cc.investor_code as source_investor_id,
        
        -- Transaction dates
        cc.call_date as transaction_date,
        cc.due_date,
        cc.payment_date as settlement_date,
        
        -- Transaction amounts in original currency
        cc.call_amount as gross_amount,
        cc.validated_call_currency as original_currency,
        
        -- Transaction breakdown
        coalesce(cc.management_fee_amount, 0) as management_fee_component,
        coalesce(cc.investment_amount, 0) as investment_component,
        coalesce(cc.expense_amount, 0) as expense_component,
        
        -- Payment tracking
        cc.payment_amount as settled_amount,
        cc.validated_payment_currency as settlement_currency,
        cc.standardized_payment_status as payment_status,
        
        -- Transaction categorization
        cc.standardized_call_purpose as transaction_purpose,
        cc.call_number as sequence_number,
        
        -- Performance metrics
        cc.payment_completion_percentage,
        cc.payment_timeliness,
        cc.days_late_early,
        cc.call_quality_rating,
        
        -- Percentages and ratios
        cc.call_percentage as commitment_percentage,
        cc.management_fee_percentage,
        cc.investment_percentage,
        cc.expense_percentage,
        
        -- Audit and metadata
        cc.created_date,
        cc.last_modified_date,
        cc.completeness_score,
        current_timestamp() as processed_at

    from admin_capital_calls cc
    left join fund_xref fx on cc.fund_code = fx.admin_fund_code
),

-- Apply currency conversion to USD base currency
currency_converted_transactions as (
    select
        *,
        
        -- Convert gross amount to USD
        case 
            when original_currency = 'USD' then gross_amount
            when fx_gross.exchange_rate is not null then gross_amount * fx_gross.exchange_rate
            else gross_amount  -- Keep original if no rate available
        end as gross_amount_usd,
        
        -- Convert settled amount to USD
        case 
            when settlement_currency = 'USD' then settled_amount
            when settlement_currency = original_currency and fx_gross.exchange_rate is not null 
                then settled_amount * fx_gross.exchange_rate
            when fx_settled.exchange_rate is not null then settled_amount * fx_settled.exchange_rate
            else settled_amount  -- Keep original if no rate available
        end as settled_amount_usd,
        
        -- Convert components to USD
        case 
            when original_currency = 'USD' then management_fee_component
            when fx_gross.exchange_rate is not null then management_fee_component * fx_gross.exchange_rate
            else management_fee_component
        end as management_fee_component_usd,
        
        case 
            when original_currency = 'USD' then investment_component
            when fx_gross.exchange_rate is not null then investment_component * fx_gross.exchange_rate
            else investment_component
        end as investment_component_usd,
        
        case 
            when original_currency = 'USD' then expense_component
            when fx_gross.exchange_rate is not null then expense_component * fx_gross.exchange_rate
            else expense_component
        end as expense_component_usd,
        
        -- Currency conversion metadata
        fx_gross.exchange_rate as gross_fx_rate,
        fx_settled.exchange_rate as settled_fx_rate,
        case 
            when original_currency != 'USD' and fx_gross.exchange_rate is not null then true
            else false
        end as currency_converted

    from capital_call_transactions cct
    left join current_fx_rates fx_gross 
        on cct.original_currency = fx_gross.from_currency 
        and fx_gross.to_currency = 'USD'
    left join current_fx_rates fx_settled 
        on cct.settlement_currency = fx_settled.from_currency 
        and fx_settled.to_currency = 'USD'
),

-- Add enhanced transaction categorization and analytics
enhanced_transactions as (
    select
        *,
        
        -- Transaction size categorization (in USD)
        case 
            when gross_amount_usd >= 50000000 then 'VERY_LARGE'
            when gross_amount_usd >= 10000000 then 'LARGE'
            when gross_amount_usd >= 1000000 then 'MEDIUM'
            when gross_amount_usd >= 100000 then 'SMALL'
            when gross_amount_usd > 0 then 'VERY_SMALL'
            else 'UNKNOWN'
        end as transaction_size_category,
        
        -- Transaction efficiency assessment
        case 
            when payment_status = 'PAID' 
                and payment_completion_percentage >= 99.9 
                and payment_timeliness in ('ON_TIME_OR_EARLY', 'SLIGHTLY_LATE')
            then 'HIGHLY_EFFICIENT'
            when payment_status = 'PAID' 
                and payment_completion_percentage >= 95
                and payment_timeliness != 'SIGNIFICANTLY_LATE'
            then 'EFFICIENT'
            when payment_status = 'PAID'
            then 'MODERATELY_EFFICIENT'
            when payment_status in ('OUTSTANDING', 'PENDING')
            then 'PENDING_ASSESSMENT'
            else 'INEFFICIENT'
        end as transaction_efficiency,
        
        -- Call composition analysis
        case 
            when investment_percentage >= 80 then 'INVESTMENT_FOCUSED'
            when management_fee_percentage >= 50 then 'FEE_FOCUSED'
            when expense_percentage >= 30 then 'EXPENSE_HEAVY'
            when investment_percentage >= 50 then 'MIXED_INVESTMENT'
            else 'BALANCED'
        end as call_composition_type,
        
        -- Settlement variance analysis
        case 
            when settled_amount_usd is not null and gross_amount_usd > 0 then
                abs(settled_amount_usd - gross_amount_usd) / gross_amount_usd * 100
            else null
        end as settlement_variance_percentage,
        
        -- Settlement quality assessment
        case 
            when payment_status = 'PAID' and settlement_variance_percentage <= 1 then 'EXACT_SETTLEMENT'
            when payment_status = 'PAID' and settlement_variance_percentage <= 5 then 'CLOSE_SETTLEMENT'
            when payment_status = 'PAID' and settlement_variance_percentage > 5 then 'VARIANCE_SETTLEMENT'
            when payment_status = 'PARTIAL' then 'PARTIAL_SETTLEMENT'
            when payment_status in ('OUTSTANDING', 'OVERDUE') then 'UNSETTLED'
            else 'UNKNOWN_SETTLEMENT'
        end as settlement_quality,
        
        -- Transaction timing analysis
        case 
            when transaction_date is not null then
                case 
                    when extract(month from transaction_date) in (1, 2, 3) then 'Q1'
                    when extract(month from transaction_date) in (4, 5, 6) then 'Q2'
                    when extract(month from transaction_date) in (7, 8, 9) then 'Q3'
                    else 'Q4'
                end
            else null
        end as transaction_quarter,
        
        extract(year from transaction_date) as transaction_year,
        
        -- Days since transaction
        case 
            when transaction_date is not null then
                datediff('day', transaction_date, current_date())
            else null
        end as days_since_transaction,
        
        -- Transaction recency classification
        case 
            when days_since_transaction <= 30 then 'RECENT'
            when days_since_transaction <= 90 then 'CURRENT'
            when days_since_transaction <= 365 then 'HISTORICAL'
            when days_since_transaction > 365 then 'ARCHIVED'
            else 'UNKNOWN'
        end as transaction_recency

    from currency_converted_transactions
),

-- Final transaction preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall transaction quality score
        (
            case when payment_status = 'PAID' then 3
                 when payment_status = 'PARTIAL' then 2
                 when payment_status in ('OUTSTANDING', 'PENDING') then 1
                 else 0 end * 0.3 +
            case when payment_timeliness = 'ON_TIME_OR_EARLY' then 3
                 when payment_timeliness = 'SLIGHTLY_LATE' then 2
                 when payment_timeliness = 'MODERATELY_LATE' then 1
                 else 0 end * 0.25 +
            case when settlement_quality = 'EXACT_SETTLEMENT' then 3
                 when settlement_quality = 'CLOSE_SETTLEMENT' then 2
                 when settlement_quality = 'VARIANCE_SETTLEMENT' then 1
                 else 0 end * 0.25 +
            case when completeness_score >= 90 then 2
                 when completeness_score >= 70 then 1
                 else 0 end * 0.2
        ) as overall_transaction_quality_score,
        
        -- Transaction processing priority
        case 
            when payment_status in ('OVERDUE', 'OUTSTANDING') and days_since_transaction > 30 then 'HIGH_PRIORITY'
            when payment_status in ('OUTSTANDING', 'PENDING') then 'MEDIUM_PRIORITY'
            when settlement_quality = 'VARIANCE_SETTLEMENT' then 'REVIEW_REQUIRED'
            else 'STANDARD'
        end as processing_priority,
        
        -- Data quality flags
        case 
            when currency_converted and (gross_fx_rate is null or settled_fx_rate is null) then 'FX_RATE_MISSING'
            when settlement_variance_percentage > 10 then 'HIGH_SETTLEMENT_VARIANCE'
            when completeness_score < 70 then 'INCOMPLETE_DATA'
            when days_late_early > 30 then 'SIGNIFICANTLY_LATE'
            else 'NO_ISSUES'
        end as data_quality_flag,
        
        -- Record hash for change detection
        hash(
            transaction_id,
            canonical_fund_id,
            source_investor_id,
            transaction_date,
            gross_amount_usd,
            payment_status,
            settlement_date,
            settled_amount_usd,
            last_modified_date
        ) as record_hash

    from enhanced_transactions
)

select * from final