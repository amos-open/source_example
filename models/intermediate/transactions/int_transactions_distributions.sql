{{
  config(
    materialized='table',
    tags=['intermediate', 'transactions']
  )
}}

/*
  Intermediate model for distribution transaction preparation
  
  This model standardizes distribution transactions from the fund administration system,
  applying consistent categorization, currency conversion logic, and tax processing
  to prepare data for canonical transaction models.
  
  Data sources:
  - Fund administration distributions (primary)
  - Fund cross-reference mappings for entity resolution
  - Currency exchange rates for conversion
  
  Business logic:
  - Standardize distribution categorization and tax treatment
  - Apply currency conversion to base currency
  - Calculate derived metrics and tax implications
  - Prepare distribution data for canonical consumption
*/

with admin_distributions as (
    select * from {{ ref('stg_admin_distributions') }}
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

-- Prepare distribution transactions with entity resolution
distribution_transactions as (
    select
        ad.distribution_id as transaction_id,
        'DISTRIBUTION' as transaction_type,
        'FUND_ADMIN_VENDOR' as source_system,
        
        -- Entity identifiers (resolved through cross-reference)
        coalesce(fx.canonical_fund_id, 'FUND-UNKNOWN-' || ad.fund_code) as canonical_fund_id,
        ad.fund_code as source_fund_id,
        ad.investor_code as source_investor_id,
        
        -- Transaction dates
        ad.distribution_date as transaction_date,
        ad.record_date,
        ad.payment_date as settlement_date,
        
        -- Transaction amounts in original currency
        ad.distribution_amount as gross_amount,
        ad.validated_distribution_currency as original_currency,
        
        -- Tax and net amounts
        coalesce(ad.withholding_tax_amount, 0) as withholding_tax_amount,
        ad.net_distribution_amount as net_amount,
        ad.calculated_net_distribution,
        ad.net_distribution_variance,
        
        -- Distribution classification
        ad.standardized_distribution_type as distribution_type,
        ad.source_investment,
        ad.tax_year,
        
        -- Cumulative tracking
        ad.cumulative_distributions,
        ad.distribution_number as sequence_number,
        
        -- Payment tracking
        ad.standardized_payment_status as payment_status,
        
        -- Performance and quality metrics
        ad.withholding_tax_rate,
        ad.payment_delay_days,
        ad.payment_timeliness,
        ad.tax_efficiency_category,
        ad.distribution_quality_rating,
        
        -- Temporal analysis
        ad.distribution_quarter,
        ad.distribution_year,
        ad.record_to_distribution_days,
        
        -- Audit and metadata
        ad.created_date,
        ad.last_modified_date,
        ad.completeness_score,
        current_timestamp() as processed_at

    from admin_distributions ad
    left join fund_xref fx on ad.fund_code = fx.admin_fund_code
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
        
        -- Convert net amount to USD
        case 
            when original_currency = 'USD' then net_amount
            when fx_gross.exchange_rate is not null then net_amount * fx_gross.exchange_rate
            else net_amount  -- Keep original if no rate available
        end as net_amount_usd,
        
        -- Convert withholding tax to USD
        case 
            when original_currency = 'USD' then withholding_tax_amount
            when fx_gross.exchange_rate is not null then withholding_tax_amount * fx_gross.exchange_rate
            else withholding_tax_amount
        end as withholding_tax_amount_usd,
        
        -- Convert cumulative distributions to USD
        case 
            when original_currency = 'USD' then cumulative_distributions
            when fx_gross.exchange_rate is not null then cumulative_distributions * fx_gross.exchange_rate
            else cumulative_distributions
        end as cumulative_distributions_usd,
        
        -- Currency conversion metadata
        fx_gross.exchange_rate as fx_rate,
        case 
            when original_currency != 'USD' and fx_gross.exchange_rate is not null then true
            else false
        end as currency_converted

    from distribution_transactions dt
    left join current_fx_rates fx_gross 
        on dt.original_currency = fx_gross.from_currency 
        and fx_gross.to_currency = 'USD'
),

-- Add enhanced transaction categorization and analytics
enhanced_transactions as (
    select
        *,
        
        -- Transaction size categorization (in USD)
        case 
            when gross_amount_usd >= 25000000 then 'VERY_LARGE'
            when gross_amount_usd >= 5000000 then 'LARGE'
            when gross_amount_usd >= 500000 then 'MEDIUM'
            when gross_amount_usd >= 50000 then 'SMALL'
            when gross_amount_usd > 0 then 'VERY_SMALL'
            else 'UNKNOWN'
        end as transaction_size_category,
        
        -- Distribution efficiency assessment
        case 
            when payment_status = 'PAID' 
                and payment_timeliness in ('ON_TIME', 'SLIGHTLY_DELAYED')
                and (net_distribution_variance is null or net_distribution_variance < 1)
            then 'HIGHLY_EFFICIENT'
            when payment_status = 'PAID' 
                and payment_timeliness != 'SIGNIFICANTLY_DELAYED'
            then 'EFFICIENT'
            when payment_status = 'PAID'
            then 'MODERATELY_EFFICIENT'
            when payment_status = 'PENDING'
            then 'PENDING_ASSESSMENT'
            else 'INEFFICIENT'
        end as transaction_efficiency,
        
        -- Tax impact analysis
        case 
            when distribution_type = 'RETURN_OF_CAPITAL' then 'TAX_DEFERRED'
            when distribution_type = 'CAPITAL_GAIN' and withholding_tax_rate <= 5 then 'LOW_TAX_IMPACT'
            when distribution_type = 'CAPITAL_GAIN' and withholding_tax_rate <= 15 then 'MODERATE_TAX_IMPACT'
            when distribution_type = 'CAPITAL_GAIN' and withholding_tax_rate > 15 then 'HIGH_TAX_IMPACT'
            when distribution_type in ('DIVIDEND_INCOME', 'INTEREST_INCOME') then 'ORDINARY_INCOME_TAX'
            when distribution_type = 'CARRIED_INTEREST' then 'CARRY_TAX_TREATMENT'
            else 'UNKNOWN_TAX_TREATMENT'
        end as tax_impact_category,
        
        -- Distribution timing analysis
        case 
            when distribution_quarter = 4 then 'YEAR_END_DISTRIBUTION'
            when distribution_quarter in (1, 2) then 'MID_YEAR_DISTRIBUTION'
            else 'REGULAR_DISTRIBUTION'
        end as distribution_timing_type,
        
        -- Net distribution efficiency
        case 
            when net_amount_usd is not null and gross_amount_usd > 0 then
                net_amount_usd / gross_amount_usd * 100
            else null
        end as net_distribution_efficiency_percentage,
        
        -- Cumulative distribution analysis
        case 
            when cumulative_distributions_usd is not null and gross_amount_usd > 0 then
                gross_amount_usd / cumulative_distributions_usd * 100
            else null
        end as current_distribution_percentage_of_cumulative,
        
        -- Distribution frequency assessment (based on sequence number and date)
        case 
            when sequence_number = 1 then 'FIRST_DISTRIBUTION'
            when sequence_number <= 5 then 'EARLY_DISTRIBUTIONS'
            when sequence_number <= 15 then 'REGULAR_DISTRIBUTIONS'
            else 'LATE_STAGE_DISTRIBUTIONS'
        end as distribution_lifecycle_stage,
        
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
        end as transaction_recency,
        
        -- Settlement variance analysis
        case 
            when net_distribution_variance is not null and gross_amount_usd > 0 then
                net_distribution_variance / gross_amount_usd * 100
            else null
        end as settlement_variance_percentage,
        
        -- Settlement quality assessment
        case 
            when payment_status = 'PAID' and settlement_variance_percentage <= 0.1 then 'EXACT_SETTLEMENT'
            when payment_status = 'PAID' and settlement_variance_percentage <= 1 then 'CLOSE_SETTLEMENT'
            when payment_status = 'PAID' and settlement_variance_percentage > 1 then 'VARIANCE_SETTLEMENT'
            when payment_status = 'PENDING' then 'UNSETTLED'
            else 'UNKNOWN_SETTLEMENT'
        end as settlement_quality

    from currency_converted_transactions
),

-- Final transaction preparation with comprehensive scoring
final as (
    select
        *,
        
        -- Overall transaction quality score
        (
            case when payment_status = 'PAID' then 3
                 when payment_status = 'PENDING' then 1
                 else 0 end * 0.3 +
            case when payment_timeliness = 'ON_TIME' then 3
                 when payment_timeliness = 'SLIGHTLY_DELAYED' then 2
                 when payment_timeliness = 'MODERATELY_DELAYED' then 1
                 else 0 end * 0.25 +
            case when settlement_quality = 'EXACT_SETTLEMENT' then 3
                 when settlement_quality = 'CLOSE_SETTLEMENT' then 2
                 when settlement_quality = 'VARIANCE_SETTLEMENT' then 1
                 else 0 end * 0.25 +
            case when completeness_score >= 90 then 2
                 when completeness_score >= 70 then 1
                 else 0 end * 0.2
        ) as overall_transaction_quality_score,
        
        -- Tax efficiency score
        (
            case when tax_efficiency_category = 'TAX_FREE' then 5
                 when tax_efficiency_category = 'LOW_TAX' then 4
                 when tax_efficiency_category = 'MODERATE_TAX' then 3
                 when tax_efficiency_category = 'HIGH_TAX' then 1
                 else 2 end +
            case when distribution_type = 'RETURN_OF_CAPITAL' then 2
                 when distribution_type = 'CAPITAL_GAIN' then 1
                 else 0 end
        ) / 7.0 * 100 as tax_efficiency_score,
        
        -- Transaction processing priority
        case 
            when payment_status = 'PENDING' and payment_delay_days > 7 then 'HIGH_PRIORITY'
            when settlement_quality = 'VARIANCE_SETTLEMENT' then 'REVIEW_REQUIRED'
            when tax_impact_category = 'HIGH_TAX_IMPACT' then 'TAX_REVIEW_REQUIRED'
            else 'STANDARD'
        end as processing_priority,
        
        -- Data quality flags
        case 
            when currency_converted and fx_rate is null then 'FX_RATE_MISSING'
            when settlement_variance_percentage > 5 then 'HIGH_SETTLEMENT_VARIANCE'
            when completeness_score < 70 then 'INCOMPLETE_DATA'
            when payment_delay_days > 30 then 'SIGNIFICANTLY_DELAYED'
            when withholding_tax_rate > 25 then 'HIGH_TAX_RATE'
            else 'NO_ISSUES'
        end as data_quality_flag,
        
        -- Investment return indicator (for capital gains distributions)
        case 
            when distribution_type = 'CAPITAL_GAIN' and transaction_size_category in ('LARGE', 'VERY_LARGE') then 'SIGNIFICANT_RETURN'
            when distribution_type = 'CAPITAL_GAIN' then 'POSITIVE_RETURN'
            when distribution_type = 'RETURN_OF_CAPITAL' then 'CAPITAL_RETURN'
            when distribution_type in ('DIVIDEND_INCOME', 'INTEREST_INCOME') then 'INCOME_DISTRIBUTION'
            else 'OTHER_DISTRIBUTION'
        end as return_indicator,
        
        -- Record hash for change detection
        hash(
            transaction_id,
            canonical_fund_id,
            source_investor_id,
            transaction_date,
            gross_amount_usd,
            distribution_type,
            payment_status,
            settlement_date,
            net_amount_usd,
            last_modified_date
        ) as record_hash

    from enhanced_transactions
)

select * from final