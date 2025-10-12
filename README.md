# AMOS Source Examples

Sample CSV datasets that mimic real private-markets systems (CRM, fund administration, portfolio management, and reference data). Use these files to stand up a demo pipeline, validate dbt models, or run end-to-end integration tests.

* Files are hosted in a public S3 bucket: `s3://amos-source-examples/…`
* Schemas are shaped to load into a `RAW` database with sub-schemas:

  * `RAW.EXAMPLE_CRM`, `RAW.EXAMPLE_ADMIN`, `RAW.EXAMPLE_PM`, `RAW.REF`

---

## What’s included

### CRM

| File                    | Target table                                | Purpose                                                                                       |
| ----------------------- | ------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `crm_opportunities.csv` | `RAW.EXAMPLE_CRM.example_crm_opportunities` | Pipeline/opportunity records with embedded company attributes, as CRMs typically return them. |

### Fund administration

| File                                  | Target table                                                | Purpose                                                               |
| ------------------------------------- | ----------------------------------------------------------- | --------------------------------------------------------------------- |
| `fund_admin_capital_calls.csv`        | `RAW.EXAMPLE_ADMIN.example_fund_admin_capital_calls`        | Capital call notices per fund/investor.                               |
| `fund_admin_distributions.csv`        | `RAW.EXAMPLE_ADMIN.example_fund_admin_distributions`        | Distributions with category (exit proceeds, dividend, interest, ROC). |
| `fund_admin_management_fees.csv`      | `RAW.EXAMPLE_ADMIN.example_fund_admin_management_fees`      | Monthly management fees.                                              |
| `fund_admin_expenses.csv`             | `RAW.EXAMPLE_ADMIN.example_fund_admin_expenses`             | Non-fee operating expenses.                                           |
| `fund_admin_investment_nav.csv`       | `RAW.EXAMPLE_ADMIN.example_fund_admin_investment_nav`       | Quarterly NAV per portfolio company per fund.                         |
| `fund_admin_fund_nav.csv`             | `RAW.EXAMPLE_ADMIN.example_fund_admin_fund_nav`             | Quarterly fund-level NAV time series.                                 |
| `fund_admin_investor_commitments.csv` | `RAW.EXAMPLE_ADMIN.example_fund_admin_investor_commitments` | Investor commitments by fund (amount, date, currency).                |

### Portfolio management

| File                         | Target table                                    | Purpose                                                                   |
| ---------------------------- | ----------------------------------------------- | ------------------------------------------------------------------------- |
| `pm_company_performance.csv` | `RAW.EXAMPLE_PM.example_pm_company_performance` | Monthly company‐level LTM KPIs (revenue, EBITDA, net income, cash, debt). |
| `pm_company_valuations.csv`  | `RAW.EXAMPLE_PM.example_pm_company_valuations`  | Annual actuals and forward forecasts by company.                          |

### Reference and cross-references

| File                | Target table                    | Purpose                                                                  |
| ------------------- | ------------------------------- | ------------------------------------------------------------------------ |
| `ref_currency.csv`  | `RAW.REF.example_ref_currency`  | ISO currency list used by examples.                                      |
| `ref_fx_rates.csv`  | `RAW.REF.example_ref_fx_rates`  | Monthly base-per-native FX rates (base USD).                             |
| `xref_fund.csv`     | `RAW.REF.example_xref_fund`     | Source fund codes → canonical `fund_id` (UUID-like), plus base currency. |
| `xref_investor.csv` | `RAW.REF.example_xref_investor` | Source investor codes → canonical `investor_id`.                         |
| `xref_company.csv`  | `RAW.REF.example_xref_company`  | Company name → canonical `company_id`, with country and currency.        |

Data span: 2019–2025, with realistic monthly/quarterly activity.

---

## Create tables (Snowflake)

Run these DDL statements once to create the target tables. Adjust database/schema names if you use different ones.

```sql
-- CRM
create or replace table RAW.EXAMPLE_CRM.example_crm_opportunities (
  opportunity_id        varchar,
  fund_code             varchar,
  opportunity_name      varchar,
  stage_name            varchar,
  stage_type            varchar,
  expected_amount       number(20,2),
  close_date            date,
  company_name          varchar,
  company_domain        varchar,
  company_country_code  varchar(2),
  industry_name         varchar,
  responsible           varchar,
  source_system         varchar,
  created_at            timestamp_ntz,
  updated_at            timestamp_ntz
);

-- ADMIN (fund admin)
create or replace table RAW.EXAMPLE_ADMIN.example_fund_admin_capital_calls (
  reference     varchar,
  fund_code     varchar,
  investor_code varchar,
  call_date     date,
  call_amount   number(20,2),
  currency_code varchar(3)
);

create or replace table RAW.EXAMPLE_ADMIN.example_fund_admin_distributions (
  reference            varchar,
  fund_code            varchar,
  investor_code        varchar,
  distribution_date    date,
  distribution_amount  number(20,2),
  currency_code        varchar(3),
  category             varchar
);

create or replace table RAW.EXAMPLE_ADMIN.example_fund_admin_management_fees (
  reference     varchar,
  fund_code     varchar,
  fee_date      date,
  fee_amount    number(20,2),
  currency_code varchar(3)
);

create or replace table RAW.EXAMPLE_ADMIN.example_fund_admin_expenses (
  reference      varchar,
  fund_code      varchar,
  expense_date   date,
  expense_amount number(20,2),
  currency_code  varchar(3)
);

create or replace table RAW.EXAMPLE_ADMIN.example_fund_admin_investment_nav (
  fund_code     varchar,
  company_name  varchar,
  as_of_date    date,
  current_nav   number(24,2),
  currency_code varchar(3)
);

create or replace table RAW.EXAMPLE_ADMIN.example_fund_admin_fund_nav (
  fund_code     varchar,
  as_of_date    date,
  nav_amount    number(24,2),
  currency_code varchar(3)
);

create or replace table RAW.EXAMPLE_ADMIN.example_fund_admin_investor_commitments (
  fund_code         varchar,
  investor_code     varchar,
  commitment_amount number(20,2),
  commitment_date   date,
  currency_code     varchar(3)
);

-- PM (portfolio mgmt)
create or replace table RAW.EXAMPLE_PM.example_pm_company_performance (
  company_name   varchar,
  date_as_of     date,
  revenue_ltm    number(24,2),
  ebitda_ltm     number(24,2),
  net_income_ltm number(24,2),
  cash           number(24,2),
  debt           number(24,2),
  currency_code  varchar(3),
  source         varchar
);

create or replace table RAW.EXAMPLE_PM.example_pm_company_valuations (
  company_name varchar,
  date_from    date,
  amount       number(20,2),
  type         varchar   -- 'ACTUAL'/'FORECAST'
);

-- REF
create or replace table RAW.REF.example_ref_currency (
  code varchar(3),
  name varchar
);

create or replace table RAW.REF.example_ref_fx_rates (
  fx_rate_as_of   date,
  base_currency   varchar(3),
  currency_code   varchar(3),
  fx_rate         number(18,8),
  fx_rate_source  varchar
);

create or replace table RAW.REF.example_xref_fund (
  fund_code          varchar,
  fund_id            varchar,
  fund_name          varchar,
  base_currency_code varchar(3)
);

create or replace table RAW.REF.example_xref_investor (
  investor_code varchar,
  investor_id   varchar,
  investor_name varchar
);

create or replace table RAW.REF.example_xref_company (
  company_name  varchar,
  company_id    varchar,
  country_code  varchar(2),
  currency_code varchar(3)
);
```

---

## Load data (Snowflake)

These commands read directly from the public S3 bucket and load into your RAW schemas.

```sql
-- One-time in your session
create or replace file format ref.csv_ff
  type = csv
  skip_header = 1
  field_optionally_enclosed_by = '"'
  empty_field_as_null = true
  null_if = ('', 'NULL');

-- CRM
copy into RAW.EXAMPLE_CRM.example_crm_opportunities
from 's3://amos-source-examples/crm_opportunities.csv'
file_format = ref.csv_ff;

-- FUND ADMIN
copy into RAW.EXAMPLE_ADMIN.example_fund_admin_capital_calls
from 's3://amos-source-examples/fund_admin_capital_calls.csv'
file_format = ref.csv_ff;

copy into RAW.EXAMPLE_ADMIN.example_fund_admin_distributions
from 's3://amos-source-examples/fund_admin_distributions.csv'
file_format = ref.csv_ff;

copy into RAW.EXAMPLE_ADMIN.example_fund_admin_expenses
from 's3://amos-source-examples/fund_admin_expenses.csv'
file_format = ref.csv_ff;

copy into RAW.EXAMPLE_ADMIN.example_fund_admin_management_fees
from 's3://amos-source-examples/fund_admin_management_fees.csv'
file_format = ref.csv_ff;

copy into RAW.EXAMPLE_ADMIN.example_fund_admin_investment_nav
from 's3://amos-source-examples/fund_admin_investment_nav.csv'
file_format = ref.csv_ff;

copy into RAW.EXAMPLE_ADMIN.example_fund_admin_fund_nav
from 's3://amos-source-examples/fund_admin_fund_nav.csv'
file_format = ref.csv_ff;

copy into RAW.EXAMPLE_ADMIN.example_fund_admin_investor_commitments
from 's3://amos-source-examples/fund_admin_investor_commitments.csv'
file_format = ref.csv_ff;

-- PORTFOLIO MGMT
copy into RAW.EXAMPLE_PM.example_pm_company_performance
from 's3://amos-source-examples/pm_company_performance.csv'
file_format = ref.csv_ff;

copy into RAW.EXAMPLE_PM.example_pm_company_valuations
from 's3://amos-source-examples/pm_company_valuations.csv'
file_format = ref.csv_ff;

-- REFERENCE
copy into RAW.REF.example_ref_currency
from 's3://amos-source-examples/ref_currency.csv'
file_format = ref.csv_ff;

copy into RAW.REF.example_ref_fx_rates
from 's3://amos-source-examples/ref_fx_rates.csv'
file_format = ref.csv_ff;

copy into RAW.REF.example_xref_fund
from 's3://amos-source-examples/xref_fund.csv'
file_format = ref.csv_ff;

copy into RAW.REF.example_xref_investor
from 's3://amos-source-examples/xref_investor.csv'
file_format = ref.csv_ff;

copy into RAW.REF.example_xref_company
from 's3://amos-source-examples/xref_company.csv'
file_format = ref.csv_ff;
```

**Re-loads:** add `force = true` to re-ingest a changed file.

**Troubleshooting**

* `403 AccessDenied`: the S3 object isn’t publicly readable. Either make the object public or use a Snowflake storage integration + external stage.
* Cast errors: these CSVs match the DDL column order/types above. If you modified tables, use a mapped `COPY` (`COPY INTO table(cols…) FROM (SELECT …)`) to reorder/cast.

---

## Using BigQuery or other warehouses

Snowflake is one implementation. You can follow the same pattern elsewhere with minor differences:

### BigQuery

* **Tables:** use `CREATE TABLE dataset.table (…)` with BigQuery types (`STRING`, `NUMERIC`, `DATE`, `TIMESTAMP`).
* **Load:** use `LOAD DATA INFILE` (beta) or more commonly a **Cloud Storage** external table / load job.

  * Example (bq CLI):

    ```bash
    bq load --source_format=CSV --skip_leading_rows=1 \
      my_dataset.example_crm_opportunities \
      gs://<bucket>/crm_opportunities.csv \
      opportunity_id:STRING,fund_code:STRING,opportunity_name:STRING,stage_name:STRING,stage_type:STRING,expected_amount:NUMERIC,close_date:DATE,company_name:STRING,company_domain:STRING,company_country_code:STRING,industry_name:STRING,responsible:STRING,source_system:STRING,created_at:TIMESTAMP,updated_at:TIMESTAMP
    ```
* **Notes:** BigQuery `NUMERIC` ~ 38 digits, 9 decimals; `BIGNUMERIC` for larger precision. Timestamps are UTC.

### Redshift

* **Tables:** standard `CREATE TABLE` with `VARCHAR`, `NUMERIC`, `DATE`, `TIMESTAMP`.
* **Load:** use `COPY` from S3 with an IAM role or keys; must specify `CSV` and `IGNOREHEADER 1`.

  ```sql
  copy schema.example_crm_opportunities
  from 's3://amos-source-examples/crm_opportunities.csv'
  iam_role 'arn:aws:iam::<acct>:role/<redshift-role>'
  csv ignoreheader 1 timeformat 'auto' dateformat 'auto';
  ```

### Databricks / Spark

* **Tables:** create managed tables with Delta Lake schemas or define external tables.
* **Load:** `spark.read.csv('s3://…', header=True)` then `.write.mode('overwrite').saveAsTable('raw.example_crm_opportunities')`.

### DuckDB (local testing)

* **Tables + Load:**

  ```sql
  create table example_crm_opportunities as
  select * from read_csv_auto('crm_opportunities.csv', header=true);
  ```

If you plan to support multiple warehouses from the same repo, keep warehouse-specific DDL and load scripts in separate folders (e.g., `warehouses/snowflake/`, `warehouses/bigquery/`) and map types carefully (`NUMBER(20,2)` → `NUMERIC`/`DECIMAL(20,2)` equivalents).

---

## How this maps to a canonical model

* `example_crm_opportunities` → canonical `opportunity`, `company`, `industry`, `country` (via cross-refs).
* `example_fund_admin_*` → canonical `transaction` (typed as drawdowns, distributions, fees, expenses), `commitment`, and NAV snapshots.
* `example_pm_*` → canonical `company_performance_snapshot` and `company_valuation`.
* `example_ref_*` and `example_xref_*` → seed dimensions (`currency`, `fx`) and entity reconciliation (fund, investor, company).

---

## License

Synthetic datasets for development and testing. Use freely in non-production environments.
