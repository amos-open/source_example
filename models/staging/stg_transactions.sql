with calls as (
  select
    reference                      as natural_key,
    fund_code,
    investor_code,
    call_date                      as date,
    call_amount                    as amount,
    currency_code,
    'DRAWDOWN'                     as transaction_type,
    'EXAMPLE'                      as source_system
  from {{ source('raw_example_admin','example_fund_admin_capital_calls') }}
),
dists as (
  select
    reference                      as natural_key,
    fund_code,
    investor_code,
    distribution_date              as date,
    distribution_amount            as amount,
    currency_code,
    case upper(category) when 'INCOME' then 'DIVIDEND' else 'DISTRIBUTION' end
                                   as transaction_type,
    'EXAMPLE'                      as source_system
  from {{ source('raw_example_admin','example_fund_admin_distributions') }}
),
fees as (
  select
    reference                      as natural_key,
    fund_code,
    null                           as investor_code,
    fee_date                       as date,
    fee_amount                     as amount,
    currency_code,
    'MANAGEMENT_FEE'               as transaction_type,
    'EXAMPLE'                      as source_system
  from {{ source('raw_example_admin','example_fund_admin_management_fees') }}
),
expenses as (
  select
    reference                      as natural_key,
    fund_code,
    null                           as investor_code,
    expense_date                   as date,
    expense_amount                 as amount,
    currency_code,
    'EXPENSE'                      as transaction_type,
    'EXAMPLE'                      as source_system
  from {{ source('raw_example_admin','example_fund_admin_expenses') }}
)
select * from (
  select * from calls
  union all select * from dists
  union all select * from fees
  union all select * from expenses
) u
