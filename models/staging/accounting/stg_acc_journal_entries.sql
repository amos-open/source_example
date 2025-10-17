{{
  config(
    materialized='view',
    tags=['accounting', 'staging']
  )
}}

/*
  Staging model for accounting journal entries data
  
  This model cleans and standardizes journal entry data from the accounting system,
  handling double-entry bookkeeping validation and transaction categorization.
  
  Transformations applied:
  - Validate and standardize financial amounts
  - Parse and validate dates
  - Standardize account codes and entry types
  - Apply double-entry bookkeeping validation
  - Handle currency standardization
  - Add transaction categorization and analysis
*/

with source as (
    select * from {{ source('accounting_vendor', 'amos_acc_journal_entries') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(journal_entry_id) as journal_entry_id,
        trim(fund_code) as fund_code,
        
        -- Entry details
        case 
            when entry_date is not null 
            then cast(entry_date as date)
            else null
        end as entry_date,
        
        trim(entry_type) as entry_type,
        trim(reference_number) as reference_number,
        trim(description) as entry_description,
        
        -- Account information
        trim(account_code) as account_code,
        trim(account_name) as account_name,
        
        -- Financial amounts
        case 
            when debit_amount is not null and debit_amount > 0
            then cast(debit_amount as number(20,2))
            else null
        end as debit_amount,
        
        case 
            when credit_amount is not null and credit_amount > 0
            then cast(credit_amount as number(20,2))
            else null
        end as credit_amount,
        
        upper(trim(currency_code)) as currency_code,
        
        -- Source and audit information
        trim(source_document) as source_document,
        trim(created_by) as created_by,
        
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
        'ACCOUNTING_VENDOR' as source_system,
        'amos_acc_journal_entries' as source_table,
        current_timestamp() as loaded_at

    from source
    where journal_entry_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when currency_code in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then currency_code
            else 'USD'  -- Default to USD for invalid codes
        end as validated_currency_code,
        
        -- Calculate net amount (debit positive, credit negative)
        case 
            when debit_amount is not null then debit_amount
            when credit_amount is not null then -credit_amount
            else 0
        end as net_amount,
        
        -- Determine entry side
        case 
            when debit_amount is not null and debit_amount > 0 then 'DEBIT'
            when credit_amount is not null and credit_amount > 0 then 'CREDIT'
            else 'UNKNOWN'
        end as entry_side,
        
        -- Get absolute amount for calculations
        coalesce(debit_amount, credit_amount, 0) as absolute_amount,
        
        -- Entry type standardization
        case 
            when upper(entry_type) like '%CAPITAL CALL%' or upper(entry_type) like '%CAPITAL%' then 'CAPITAL_CALL'
            when upper(entry_type) like '%INVESTMENT%' or upper(entry_type) like '%PURCHASE%' then 'INVESTMENT'
            when upper(entry_type) like '%DISTRIBUTION%' or upper(entry_type) like '%DIVIDEND%' then 'DISTRIBUTION'
            when upper(entry_type) like '%MANAGEMENT FEE%' or upper(entry_type) like '%FEE%' then 'MANAGEMENT_FEE'
            when upper(entry_type) like '%EXPENSE%' or upper(entry_type) like '%COST%' then 'EXPENSE'
            when upper(entry_type) like '%INTEREST%' then 'INTEREST'
            when upper(entry_type) like '%VALUATION%' or upper(entry_type) like '%MARK%' then 'VALUATION_ADJUSTMENT'
            when upper(entry_type) like '%REALIZED%' or upper(entry_type) like '%SALE%' then 'REALIZED_GAIN_LOSS'
            when upper(entry_type) like '%UNREALIZED%' then 'UNREALIZED_GAIN_LOSS'
            else 'OTHER'
        end as standardized_entry_type,
        
        -- Account classification based on account code patterns
        case 
            when account_code like '11%' then 'CASH_AND_EQUIVALENTS'
            when account_code like '112%' then 'INVESTMENTS'
            when account_code like '12%' then 'OTHER_ASSETS'
            when account_code like '2%' then 'LIABILITIES'
            when account_code like '31%' then 'PARTNER_CAPITAL'
            when account_code like '32%' then 'RETAINED_EARNINGS'
            when account_code like '4%' then 'INCOME'
            when account_code like '5%' then 'EXPENSES'
            when account_code like '6%' then 'GAINS_LOSSES'
            else 'OTHER'
        end as account_classification,
        
        -- Determine if this is a balance sheet or income statement account
        case 
            when account_code like '1%' or account_code like '2%' or account_code like '3%' then 'BALANCE_SHEET'
            when account_code like '4%' or account_code like '5%' or account_code like '6%' then 'INCOME_STATEMENT'
            else 'OTHER'
        end as financial_statement_category,
        
        -- Extract period information
        year(entry_date) as entry_year,
        quarter(entry_date) as entry_quarter,
        month(entry_date) as entry_month,
        
        -- Transaction size categorization
        case 
            when absolute_amount >= 100000000 then 'VERY_LARGE'  -- $100M+
            when absolute_amount >= 10000000 then 'LARGE'        -- $10M-$100M
            when absolute_amount >= 1000000 then 'MEDIUM'        -- $1M-$10M
            when absolute_amount >= 100000 then 'SMALL'          -- $100K-$1M
            when absolute_amount > 0 then 'VERY_SMALL'           -- <$100K
            else 'ZERO'
        end as transaction_size_category,
        
        -- Data quality checks
        case 
            when debit_amount is not null and credit_amount is not null then 'BOTH_DEBIT_CREDIT'
            when debit_amount is null and credit_amount is null then 'NO_AMOUNT'
            else 'VALID'
        end as amount_validation_status,
        
        -- Source document type inference
        case 
            when upper(source_document) like '%CC-%' then 'CAPITAL_CALL'
            when upper(source_document) like '%INV-%' then 'INVESTMENT'
            when upper(source_document) like '%DIST-%' then 'DISTRIBUTION'
            when upper(source_document) like '%MF-%' then 'MANAGEMENT_FEE'
            when upper(source_document) like '%EXP-%' then 'EXPENSE'
            else 'OTHER'
        end as inferred_document_type

    from cleaned
),

final as (
    select
        *,
        
        -- Overall entry quality assessment
        case 
            when amount_validation_status = 'VALID'
                and account_code is not null
                and entry_date is not null
                and standardized_entry_type != 'OTHER'
            then 'HIGH_QUALITY'
            when amount_validation_status = 'VALID'
                and account_code is not null
                and entry_date is not null
            then 'MEDIUM_QUALITY'
            when amount_validation_status = 'VALID'
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as entry_quality_rating,
        
        -- Business process categorization
        case 
            when standardized_entry_type in ('CAPITAL_CALL', 'DISTRIBUTION') then 'INVESTOR_TRANSACTIONS'
            when standardized_entry_type in ('INVESTMENT', 'REALIZED_GAIN_LOSS', 'UNREALIZED_GAIN_LOSS', 'VALUATION_ADJUSTMENT') then 'INVESTMENT_TRANSACTIONS'
            when standardized_entry_type in ('MANAGEMENT_FEE', 'EXPENSE') then 'OPERATIONAL_TRANSACTIONS'
            when standardized_entry_type = 'INTEREST' then 'FINANCING_TRANSACTIONS'
            else 'OTHER_TRANSACTIONS'
        end as business_process_category,
        
        -- Completeness assessment
        (
            case when fund_code is not null then 1 else 0 end +
            case when entry_date is not null then 1 else 0 end +
            case when account_code is not null then 1 else 0 end +
            case when absolute_amount > 0 then 1 else 0 end +
            case when entry_type is not null then 1 else 0 end +
            case when reference_number is not null then 1 else 0 end +
            case when entry_description is not null then 1 else 0 end +
            case when source_document is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        hash(
            journal_entry_id,
            fund_code,
            entry_date,
            account_code,
            debit_amount,
            credit_amount,
            entry_type,
            reference_number,
            last_modified_date
        ) as record_hash

    from enhanced
)

select * from final