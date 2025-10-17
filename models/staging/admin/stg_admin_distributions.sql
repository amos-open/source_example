{{
  config(
    materialized='view',
    tags=['fund_admin', 'staging']
  )
}}

/*
  Staging model for fund administration distributions data
  
  This model cleans and standardizes distribution transaction data from the fund administration system,
  handling financial amounts, tax implications, and payment tracking.
  
  Transformations applied:
  - Validate and standardize financial amounts
  - Parse and validate dates
  - Standardize currency codes and distribution types
  - Calculate tax-related metrics
  - Handle payment status and tracking
  - Add distribution categorization and analysis
*/

with source as (
    select * from {{ source('fund_admin_vendor', 'amos_admin_distributions') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(distribution_id) as distribution_id,
        trim(fund_code) as fund_code,
        trim(investor_code) as investor_code,
        
        -- Distribution details
        case 
            when distribution_number is not null and distribution_number > 0
            then cast(distribution_number as number(5,0))
            else null
        end as distribution_number,
        
        -- Dates
        case 
            when distribution_date is not null 
            then cast(distribution_date as date)
            else null
        end as distribution_date,
        
        case 
            when record_date is not null 
            then cast(record_date as date)
            else null
        end as record_date,
        
        case 
            when payment_date is not null 
            then cast(payment_date as date)
            else null
        end as payment_date,
        
        -- Financial amounts
        case 
            when distribution_amount is not null and distribution_amount > 0
            then cast(distribution_amount as number(20,2))
            else null
        end as distribution_amount,
        
        upper(trim(distribution_currency)) as distribution_currency,
        
        case 
            when withholding_tax_amount is not null and withholding_tax_amount >= 0
            then cast(withholding_tax_amount as number(20,2))
            else null
        end as withholding_tax_amount,
        
        case 
            when net_distribution_amount is not null and net_distribution_amount >= 0
            then cast(net_distribution_amount as number(20,2))
            else null
        end as net_distribution_amount,
        
        case 
            when cumulative_distributions is not null and cumulative_distributions >= 0
            then cast(cumulative_distributions as number(20,2))
            else null
        end as cumulative_distributions,
        
        -- Distribution classification
        trim(distribution_type) as distribution_type,
        trim(source_investment) as source_investment,
        
        case 
            when tax_year is not null 
                and tax_year between 2000 and year(current_date()) + 1
            then cast(tax_year as number(4,0))
            else null
        end as tax_year,
        
        -- Status tracking
        upper(trim(status)) as payment_status,
        
        -- Audit fields
        case 
            when created_date is not null 
            then cast(created_date as date)
            else null
        end as created_date,
        
        case 
            when last_modified_date is not null 
            then cast(last_modified_date as date)
            else null
        end as last_modified_date,
        
        -- Source system metadata
        'FUND_ADMIN_VENDOR' as source_system,
        'amos_admin_distributions' as source_table,
        current_timestamp() as loaded_at

    from source
    where distribution_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when distribution_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then distribution_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_distribution_currency,
        
        -- Distribution type standardization
        case 
            when upper(distribution_type) like '%RETURN OF CAPITAL%' 
                or upper(distribution_type) like '%ROC%' then 'RETURN_OF_CAPITAL'
            when upper(distribution_type) like '%CAPITAL GAIN%' 
                or upper(distribution_type) like '%REALIZED GAIN%' then 'CAPITAL_GAIN'
            when upper(distribution_type) like '%DIVIDEND%' 
                or upper(distribution_type) like '%INCOME%' then 'DIVIDEND_INCOME'
            when upper(distribution_type) like '%INTEREST%' then 'INTEREST_INCOME'
            when upper(distribution_type) like '%FEE%' 
                or upper(distribution_type) like '%CARRY%' then 'CARRIED_INTEREST'
            else 'OTHER'
        end as standardized_distribution_type,
        
        -- Tax implications
        case 
            when distribution_amount is not null and distribution_amount > 0 then
                coalesce(withholding_tax_amount, 0) / distribution_amount * 100
            else null
        end as withholding_tax_rate,
        
        -- Validate net distribution calculation
        case 
            when distribution_amount is not null and withholding_tax_amount is not null
            then distribution_amount - withholding_tax_amount
            else distribution_amount
        end as calculated_net_distribution,
        
        -- Check for calculation discrepancies
        case 
            when net_distribution_amount is not null and calculated_net_distribution is not null
            then abs(net_distribution_amount - calculated_net_distribution)
            else null
        end as net_distribution_variance,
        
        -- Payment timing analysis
        case 
            when distribution_date is not null and payment_date is not null
            then datediff('day', distribution_date, payment_date)
            else null
        end as payment_delay_days,
        
        case 
            when record_date is not null and distribution_date is not null
            then datediff('day', record_date, distribution_date)
            else null
        end as record_to_distribution_days,
        
        -- Payment status standardization
        case 
            when payment_status in ('PAID', 'COMPLETED', 'PROCESSED') then 'PAID'
            when payment_status in ('PENDING', 'SCHEDULED') then 'PENDING'
            when payment_status in ('CANCELLED', 'VOID') then 'CANCELLED'
            when payment_status in ('FAILED', 'REJECTED') then 'FAILED'
            else 'UNKNOWN'
        end as standardized_payment_status,
        
        -- Distribution timing assessment
        case 
            when standardized_payment_status = 'PAID' then
                case 
                    when payment_delay_days <= 0 then 'ON_TIME'
                    when payment_delay_days <= 3 then 'SLIGHTLY_DELAYED'
                    when payment_delay_days <= 7 then 'MODERATELY_DELAYED'
                    else 'SIGNIFICANTLY_DELAYED'
                end
            else 'N/A'
        end as payment_timeliness,
        
        -- Quarter and year extraction for reporting
        quarter(distribution_date) as distribution_quarter,
        year(distribution_date) as distribution_year,
        
        -- Tax efficiency assessment
        case 
            when withholding_tax_rate is null or withholding_tax_rate = 0 then 'TAX_FREE'
            when withholding_tax_rate <= 5 then 'LOW_TAX'
            when withholding_tax_rate <= 15 then 'MODERATE_TAX'
            else 'HIGH_TAX'
        end as tax_efficiency_category

    from cleaned
),

final as (
    select
        *,
        
        -- Overall distribution quality assessment
        case 
            when standardized_payment_status = 'PAID' 
                and payment_timeliness in ('ON_TIME', 'SLIGHTLY_DELAYED')
                and (net_distribution_variance is null or net_distribution_variance < 1)
            then 'EXCELLENT'
            when standardized_payment_status = 'PAID' 
                and payment_timeliness != 'SIGNIFICANTLY_DELAYED'
            then 'GOOD'
            when standardized_payment_status = 'PAID'
            then 'FAIR'
            else 'POOR'
        end as distribution_quality_rating,
        
        -- Data completeness assessment
        (
            case when fund_code is not null then 1 else 0 end +
            case when investor_code is not null then 1 else 0 end +
            case when distribution_date is not null then 1 else 0 end +
            case when distribution_amount is not null then 1 else 0 end +
            case when distribution_currency is not null then 1 else 0 end +
            case when distribution_type is not null then 1 else 0 end +
            case when payment_status is not null then 1 else 0 end +
            case when tax_year is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        hash(
            distribution_id,
            fund_code,
            investor_code,
            distribution_date,
            distribution_amount,
            distribution_type,
            payment_status,
            payment_date,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final