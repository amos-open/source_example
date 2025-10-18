{{
  config(
    materialized='view',
    tags=['fund_admin', 'staging']
  )
}}

/*
  Staging model for fund administration capital calls data
  
  This model cleans and standardizes capital call transaction data from the fund administration system,
  handling financial amounts, dates, and payment tracking.
  
  Transformations applied:
  - Validate and standardize financial amounts
  - Parse and validate dates
  - Standardize currency codes
  - Calculate derived metrics (percentages, timing)
  - Handle payment status and tracking
  - Add transaction categorization
*/

with source as (
    select * from {{ source('fund_admin_vendor', 'amos_admin_capital_calls') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(capital_call_id) as capital_call_id,
        trim(fund_code) as fund_code,
        trim(investor_code) as investor_code,
        
        -- Call details
        case 
            when call_number is not null and call_number > 0
            then CAST(call_number AS NUMERIC(5,0))
            else null
        end as call_number,
        
        -- Dates
        case 
            when call_date is not null 
            then CAST(call_date AS DATE)
            else null
        end as call_date,
        
        case 
            when due_date is not null 
            then CAST(due_date AS DATE)
            else null
        end as due_date,
        
        case 
            when payment_date is not null 
            then CAST(payment_date AS DATE)
            else null
        end as payment_date,
        
        -- Financial amounts
        case 
            when call_amount is not null and call_amount > 0
            then CAST(call_amount AS NUMERIC(20,2))
            else null
        end as call_amount,
        
        upper(trim(call_currency)) as call_currency,
        
        case 
            when commitment_amount is not null and commitment_amount > 0
            then CAST(commitment_amount AS NUMERIC(20,2))
            else null
        end as commitment_amount,
        
        upper(trim(commitment_currency)) as commitment_currency,
        
        case 
            when call_percentage is not null 
                and call_percentage between 0 and 100
            then CAST(call_percentage AS NUMERIC(8,4))
            else null
        end as call_percentage,
        
        -- Call purpose and breakdown
        trim(purpose) as call_purpose,
        
        case 
            when management_fee_amount is not null and management_fee_amount >= 0
            then CAST(management_fee_amount AS NUMERIC(20,2))
            else null
        end as management_fee_amount,
        
        case 
            when investment_amount is not null and investment_amount >= 0
            then CAST(investment_amount AS NUMERIC(20,2))
            else null
        end as investment_amount,
        
        case 
            when expense_amount is not null and expense_amount >= 0
            then CAST(expense_amount AS NUMERIC(20,2))
            else null
        end as expense_amount,
        
        -- Payment tracking
        upper(trim(status)) as payment_status,
        
        case 
            when payment_amount is not null and payment_amount >= 0
            then CAST(payment_amount AS NUMERIC(20,2))
            else null
        end as payment_amount,
        
        upper(trim(payment_currency)) as payment_currency,
        
        -- Audit fields
        case 
            when created_date is not null 
            then CAST(created_date AS DATE)
            else null
        end as created_date,
        
        case 
            when last_modified_date is not null 
            then CAST(last_modified_date AS DATE)
            else null
        end as last_modified_date,
        
        -- Source system metadata
        'FUND_ADMIN_VENDOR' as source_system,
        'amos_admin_capital_calls' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where capital_call_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when call_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then call_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_call_currency,
        
        case 
            when payment_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then payment_currency
            else call_currency  -- Default to call currency
        end as validated_payment_currency,
        
        -- Calculate timing metrics
        case 
            when call_date is not null and due_date is not null
            then DATE_DIFF(due_date, call_date, DAY)
            else null
        end as payment_period_days,
        
        case 
            when due_date is not null and payment_date is not null
            then DATE_DIFF(payment_date, due_date, DAY)
            else null
        end as days_late_early,  -- Positive = late, negative = early
        
        case 
            when call_date is not null and payment_date is not null
            then DATE_DIFF(payment_date, call_date, DAY)
            else null
        end as total_payment_days,
        
        -- Payment performance metrics
        case 
            when payment_amount is not null and call_amount is not null and call_amount > 0
            then (payment_amount / call_amount) * 100
            else null
        end as payment_completion_percentage,
        
        -- Call composition analysis
        case 
            when call_amount is not null and call_amount > 0 then
                coalesce(management_fee_amount, 0) / call_amount * 100
            else null
        end as management_fee_percentage,
        
        case 
            when call_amount is not null and call_amount > 0 then
                coalesce(investment_amount, 0) / call_amount * 100
            else null
        end as investment_percentage,
        
        case 
            when call_amount is not null and call_amount > 0 then
                coalesce(expense_amount, 0) / call_amount * 100
            else null
        end as expense_percentage,
        
        -- Call purpose standardization
        case 
            when upper(call_purpose) like '%INITIAL%' or upper(call_purpose) like '%FIRST%' then 'INITIAL_INVESTMENT'
            when upper(call_purpose) like '%PORTFOLIO%' or upper(call_purpose) like '%INVESTMENT%' then 'PORTFOLIO_INVESTMENT'
            when upper(call_purpose) like '%FOLLOW%' or upper(call_purpose) like '%ADDITIONAL%' then 'FOLLOW_ON_INVESTMENT'
            when upper(call_purpose) like '%MANAGEMENT%' or upper(call_purpose) like '%FEE%' then 'MANAGEMENT_FEES'
            when upper(call_purpose) like '%EXPENSE%' or upper(call_purpose) like '%COST%' then 'FUND_EXPENSES'
            else 'OTHER'
        end as standardized_call_purpose,
        
        -- Payment status standardization
        case 
            when payment_status in ('PAID', 'COMPLETED', 'RECEIVED') then 'PAID'
            when payment_status in ('PENDING', 'OUTSTANDING', 'CALLED') then 'OUTSTANDING'
            when payment_status in ('OVERDUE', 'LATE') then 'OVERDUE'
            when payment_status in ('PARTIAL', 'PARTIALLY_PAID') then 'PARTIAL'
            when payment_status in ('CANCELLED', 'VOID') then 'CANCELLED'
            else 'UNKNOWN'
        end as standardized_payment_status,
        
        -- Payment timeliness assessment
        case 
            when standardized_payment_status = 'PAID' then
                case 
                    when days_late_early <= 0 then 'ON_TIME_OR_EARLY'
                    when days_late_early <= 5 then 'SLIGHTLY_LATE'
                    when days_late_early <= 15 then 'MODERATELY_LATE'
                    else 'SIGNIFICANTLY_LATE'
                end
            when standardized_payment_status = 'OVERDUE' then 'OVERDUE'
            when standardized_payment_status = 'OUTSTANDING' and due_date < current_date() then 'OVERDUE'
            when standardized_payment_status = 'OUTSTANDING' then 'PENDING'
            else 'N/A'
        end as payment_timeliness

    from cleaned
),

final as (
    select
        *,
        
        -- Overall call quality score
        case 
            when standardized_payment_status = 'PAID' 
                and payment_completion_percentage >= 99.9 
                and payment_timeliness in ('ON_TIME_OR_EARLY', 'SLIGHTLY_LATE')
            then 'EXCELLENT'
            when standardized_payment_status = 'PAID' 
                and payment_completion_percentage >= 95
            then 'GOOD'
            when standardized_payment_status in ('PAID', 'PARTIAL')
            then 'FAIR'
            else 'POOR'
        end as call_quality_rating,
        
        -- Data completeness assessment
        (
            case when fund_code is not null then 1 else 0 end +
            case when investor_code is not null then 1 else 0 end +
            case when call_date is not null then 1 else 0 end +
            case when due_date is not null then 1 else 0 end +
            case when call_amount is not null then 1 else 0 end +
            case when call_currency is not null then 1 else 0 end +
            case when call_purpose is not null then 1 else 0 end +
            case when payment_status is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        FARM_FINGERPRINT(CONCAT(capital_call_id, fund_code, investor_code, call_date, call_amount, payment_status, payment_date, payment_amount, last_modified_date)) as record_hash

    from enhanced
)

select * from final