{{
  config(
    materialized='view',
    tags=['accounting', 'staging']
  )
}}

/*
  Staging model for accounting bank transactions data
  
  This model cleans and standardizes bank transaction data from the accounting system,
  handling cash flow categorization and reconciliation tracking.
  
  Transformations applied:
  - Validate and standardize financial amounts
  - Parse and validate dates
  - Standardize transaction types and counterparties
  - Handle reconciliation status tracking
  - Add cash flow categorization and analysis
*/

with source as (
    select * from {{ source('accounting_vendor', 'amos_acc_bank_transactions') }}
),

cleaned as (
    select
        -- Primary identifiers
        trim(transaction_id) as transaction_id,
        trim(fund_code) as fund_code,
        
        -- Bank account information
        trim(bank_account_number) as bank_account_number,
        
        -- Transaction details
        case 
            when transaction_date is not null 
            then CAST(transaction_date AS DATE)
            else null
        end as transaction_date,
        
        trim(transaction_type) as transaction_type,
        
        case 
            when transaction_amount is not null 
            then CAST(transaction_amount AS NUMERIC(20,2))
            else null
        end as transaction_amount,
        
        upper(trim(transaction_currency)) as transaction_currency,
        
        -- Counterparty information
        trim(counterparty_name) as counterparty_name,
        trim(counterparty_account) as counterparty_account,
        
        -- Transaction description and references
        trim(description) as transaction_description,
        trim(reference_number) as reference_number,
        
        -- Reconciliation tracking
        upper(trim(reconciliation_status)) as reconciliation_status,
        trim(journal_entry_id) as journal_entry_id,
        
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
        'ACCOUNTING_VENDOR' as source_system,
        'amos_acc_bank_transactions' as source_table,
        CURRENT_TIMESTAMP() as loaded_at

    from source
    where transaction_id is not null  -- Filter out records without primary key
),

enhanced as (
    select
        *,
        
        -- Standardize currency codes
        case 
            when transaction_currency in ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'SGD')
            then transaction_currency
            else 'USD'  -- Default to USD for invalid codes
        end as validated_transaction_currency,
        
        -- Determine transaction direction and sign
        case 
            when upper(transaction_type) like '%IN%' 
                or upper(transaction_type) like '%DEPOSIT%' 
                or upper(transaction_type) like '%CREDIT%' then 'INFLOW'
            when upper(transaction_type) like '%OUT%' 
                or upper(transaction_type) like '%WITHDRAWAL%' 
                or upper(transaction_type) like '%DEBIT%' then 'OUTFLOW'
            else 'UNKNOWN'
        end as cash_flow_direction,
        
        -- Apply appropriate sign based on direction
        case 
            when upper(transaction_type) like '%IN%' 
                or upper(transaction_type) like '%DEPOSIT%' 
                or upper(transaction_type) like '%CREDIT%' then abs(transaction_amount)
            when upper(transaction_type) like '%OUT%' 
                or upper(transaction_type) like '%WITHDRAWAL%' 
                or upper(transaction_type) like '%DEBIT%' then -abs(transaction_amount)
            else transaction_amount
        end as signed_transaction_amount,
        
        -- Transaction type standardization
        case 
            when upper(transaction_type) like '%WIRE%' then 'WIRE_TRANSFER'
            when upper(transaction_type) like '%ACH%' then 'ACH_TRANSFER'
            when upper(transaction_type) like '%CHECK%' then 'CHECK'
            when upper(transaction_type) like '%DEPOSIT%' then 'DEPOSIT'
            when upper(transaction_type) like '%FEE%' then 'BANK_FEE'
            when upper(transaction_type) like '%INTEREST%' then 'INTEREST'
            else 'OTHER'
        end as standardized_transaction_type,
        
        -- Reconciliation status standardization
        case 
            when reconciliation_status in ('RECONCILED', 'MATCHED', 'CLEARED') then 'RECONCILED'
            when reconciliation_status in ('PENDING', 'UNMATCHED') then 'PENDING'
            when reconciliation_status in ('EXCEPTION', 'ERROR', 'FAILED') then 'EXCEPTION'
            else 'UNKNOWN'
        end as standardized_reconciliation_status,
        
        -- Business purpose categorization based on description and counterparty
        case 
            when upper(transaction_description) like '%CAPITAL CALL%' 
                or upper(counterparty_name) like '%PENSION%' 
                or upper(counterparty_name) like '%ENDOWMENT%' 
                or upper(counterparty_name) like '%INVESTOR%' then 'CAPITAL_CALL'
            when upper(transaction_description) like '%DISTRIBUTION%' 
                or upper(transaction_description) like '%DIVIDEND%' then 'DISTRIBUTION'
            when upper(transaction_description) like '%INVESTMENT%' 
                or upper(transaction_description) like '%ACQUISITION%' then 'INVESTMENT'
            when upper(transaction_description) like '%MANAGEMENT FEE%' 
                or upper(counterparty_name) like '%MANAGEMENT%' then 'MANAGEMENT_FEE'
            when upper(transaction_description) like '%EXPENSE%' 
                or upper(transaction_description) like '%LEGAL%' 
                or upper(transaction_description) like '%AUDIT%' then 'OPERATING_EXPENSE'
            when upper(transaction_type) like '%FEE%' then 'BANK_FEE'
            when upper(transaction_type) like '%INTEREST%' then 'INTEREST'
            else 'OTHER'
        end as business_purpose_category,
        
        -- Counterparty type classification
        case 
            when upper(counterparty_name) like '%PENSION%' 
                or upper(counterparty_name) like '%RETIREMENT%' then 'PENSION_FUND'
            when upper(counterparty_name) like '%ENDOWMENT%' 
                or upper(counterparty_name) like '%UNIVERSITY%' then 'ENDOWMENT'
            when upper(counterparty_name) like '%FOUNDATION%' then 'FOUNDATION'
            when upper(counterparty_name) like '%INSURANCE%' then 'INSURANCE_COMPANY'
            when upper(counterparty_name) like '%MANAGEMENT%' 
                or upper(counterparty_name) like '%ADVISOR%' then 'FUND_MANAGER'
            when upper(counterparty_name) like '%BANK%' 
                or upper(counterparty_name) like '%TRUST%' then 'FINANCIAL_INSTITUTION'
            when upper(counterparty_name) like '%LLC%' 
                or upper(counterparty_name) like '%INC%' 
                or upper(counterparty_name) like '%CORP%' then 'PORTFOLIO_COMPANY'
            when upper(counterparty_name) like '%LAW%' 
                or upper(counterparty_name) like '%LEGAL%' then 'LEGAL_SERVICES'
            when upper(counterparty_name) like '%AUDIT%' 
                or upper(counterparty_name) like '%ACCOUNTING%' then 'ACCOUNTING_SERVICES'
            else 'OTHER'
        end as counterparty_type,
        
        -- Transaction size categorization
        case 
            when abs(transaction_amount) >= 100000000 then 'VERY_LARGE'  -- $100M+
            when abs(transaction_amount) >= 10000000 then 'LARGE'        -- $10M-$100M
            when abs(transaction_amount) >= 1000000 then 'MEDIUM'        -- $1M-$10M
            when abs(transaction_amount) >= 100000 then 'SMALL'          -- $100K-$1M
            when abs(transaction_amount) > 0 then 'VERY_SMALL'           -- <$100K
            else 'ZERO'
        end as transaction_size_category,
        
        -- Time-based analysis
        year(transaction_date) as transaction_year,
        quarter(transaction_date) as transaction_quarter,
        month(transaction_date) as transaction_month,
        dayofweek(transaction_date) as transaction_day_of_week,
        
        -- Bank account masking for privacy (keep last 4 digits)
        case 
            when bank_account_number is not null and length(bank_account_number) > 4
            then 'XXXX-XXXX-' || right(bank_account_number, 4)
            else bank_account_number
        end as masked_bank_account,
        
        -- Reconciliation timing
        case 
            when created_date is not null and transaction_date is not null
            then DATEDIFF('day', transaction_date, created_date)
            else null
        end as days_to_record,
        
        -- Reference number pattern analysis
        case 
            when reference_number like 'CC-%' then 'CAPITAL_CALL_REF'
            when reference_number like 'INV-%' then 'INVESTMENT_REF'
            when reference_number like 'DIST-%' then 'DISTRIBUTION_REF'
            when reference_number like 'MF-%' then 'MANAGEMENT_FEE_REF'
            when reference_number like 'EXP-%' then 'EXPENSE_REF'
            else 'OTHER_REF'
        end as reference_pattern_type

    from cleaned
),

final as (
    select
        *,
        
        -- Overall transaction quality assessment
        case 
            when standardized_reconciliation_status = 'RECONCILED'
                and journal_entry_id is not null
                and business_purpose_category != 'OTHER'
                and counterparty_name is not null
            then 'HIGH_QUALITY'
            when standardized_reconciliation_status in ('RECONCILED', 'PENDING')
                and transaction_amount is not null
                and transaction_date is not null
            then 'MEDIUM_QUALITY'
            when transaction_amount is not null
                and transaction_date is not null
            then 'LOW_QUALITY'
            else 'POOR_QUALITY'
        end as transaction_quality_rating,
        
        -- Cash flow impact assessment
        case 
            when business_purpose_category in ('CAPITAL_CALL') and cash_flow_direction = 'INFLOW' then 'POSITIVE_FUNDING'
            when business_purpose_category in ('INVESTMENT') and cash_flow_direction = 'OUTFLOW' then 'INVESTMENT_DEPLOYMENT'
            when business_purpose_category in ('DISTRIBUTION') and cash_flow_direction = 'OUTFLOW' then 'INVESTOR_RETURN'
            when business_purpose_category in ('MANAGEMENT_FEE', 'OPERATING_EXPENSE') and cash_flow_direction = 'OUTFLOW' then 'OPERATIONAL_COST'
            when business_purpose_category = 'INTEREST' and cash_flow_direction = 'INFLOW' then 'INCOME_GENERATION'
            else 'OTHER_IMPACT'
        end as cash_flow_impact_type,
        
        -- Completeness assessment
        (
            case when fund_code is not null then 1 else 0 end +
            case when transaction_date is not null then 1 else 0 end +
            case when transaction_amount is not null then 1 else 0 end +
            case when transaction_type is not null then 1 else 0 end +
            case when counterparty_name is not null then 1 else 0 end +
            case when transaction_description is not null then 1 else 0 end +
            case when reconciliation_status is not null then 1 else 0 end +
            case when reference_number is not null then 1 else 0 end
        ) / 8.0 * 100 as completeness_score,
        
        -- Record hash for change detection
        TO_VARCHAR(MD5(CONCAT(
          COALESCE(transaction_id,''),
          COALESCE(fund_code,''),
          COALESCE(TO_VARCHAR(transaction_date),''),
          COALESCE(TO_VARCHAR(transaction_amount),''),
          COALESCE(transaction_type,''),
          COALESCE(counterparty_name,''),
          COALESCE(reconciliation_status,''),
          COALESCE(journal_entry_id,''),
          COALESCE(TO_VARCHAR(last_modified_date),'')
        ))) as record_hash

    from enhanced
)

select * from final