# Integration Tests for AMOS Core Compatibility

This directory contains integration tests that validate compatibility between the amos_source_example package and the amos_core canonical models.

## Test Files

### 1. `test_amos_core_compatibility.sql`
Basic schema compatibility test that attempts to select the exact columns expected by amos_core from the intermediate models.

### 2. `test_data_contracts.sql`
Validates that the data produced meets the business rules and constraints expected by amos_core (e.g., required fields, data types, business logic).

### 3. `test_missing_columns.sql`
Identifies which columns are missing from intermediate models that are required by amos_core canonical models.

### 4. `test_column_mapping_analysis.sql`
Analyzes the availability of specific columns and identifies mapping issues between staging and canonical models.

### 5. `test_schema_compatibility_fixed.sql`
Tests the schema compatibility after applying fixes to the seed data and intermediate models.

### 6. `test_amos_core_contract_validation.sql`
Comprehensive validation of data contracts including UUID formats, field lengths, and business rules.

## Schema Fixes Applied

### Company Entity (`int_entities_company`)
- **Added missing columns to seed data:**
  - `base_currency_code`: Added to CRM companies CSV with appropriate currency codes (USD, GBP, EUR, SGD)
  - `industry_id`: Added to CRM companies CSV with structured industry IDs (IND-TECH-001, IND-HLTH-001, etc.)

- **Updated staging model (`stg_crm_companies.sql`):**
  - Added processing for `base_currency_code` and `industry_id` columns
  - Maintained existing data cleaning and validation logic

- **Fixed intermediate model (`int_entities_company.sql`):**
  - Updated final select to use actual `base_currency_code` from staging instead of hardcoded 'USD'
  - Updated to use actual `industry_id` from staging instead of null

### Investor Entity (`int_entities_investor`)
- **Added missing columns to seed data:**
  - `investor_type_id`: Added to admin investors CSV with structured type IDs (INVTYPE-PENSION-001, INVTYPE-ENDOW-001)

- **Updated staging model (`stg_admin_investors.sql`):**
  - Added processing for `investor_type_id` column

### Fund Entity (`int_entities_fund`)
- **No changes required:** Fund seed data already contained all necessary columns

### Counterparty Entity (`int_entities_counterparty`)
- **No changes required:** Counterparty model generates data from existing sources

## Remaining Issues to Address

### 1. SQL Syntax Compatibility (dbt-fusion)
The project uses dbt-fusion which has different syntax requirements:
- **Regex patterns:** Replace `~` operators with `REGEXP_LIKE()` function calls
- **Test syntax:** Update deprecated test argument formats in schema.yml files

### 2. UUID Generation
The intermediate models need to generate proper UUID values for entity IDs:
- Consider using `UUID()` function or similar for generating canonical IDs
- Ensure UUID format matches the pattern expected by amos_core contracts

### 3. Reference Data Dependencies
Some models reference lookup tables that may not exist:
- `currency` table for currency code validation
- `industry` table for industry_id foreign key relationships
- `country` table for country code validation
- `investor_type` table for investor_type_id relationships

### 4. Data Type Precision
Ensure numeric fields match exact precision requirements:
- `decimal(7,4)` for fee rates
- `numeric(20,2)` for monetary amounts
- `char(2)` vs `varchar(2)` for country codes

## Running the Tests

To run these integration tests:

```bash
# Run all integration tests
dbt test --select tag:integration

# Run specific test categories
dbt test --select tag:amos_core_compatibility
dbt test --select tag:contract_validation
dbt test --select tag:schema_fixed

# Run tests for specific models
dbt test --select int_entities_company
```

## Expected Test Results

After applying the fixes:
- `test_schema_compatibility_fixed.sql` should show all entities as PASS
- `test_amos_core_contract_validation.sql` should show minimal violations
- Remaining failures will likely be related to UUID format and reference data dependencies

## Next Steps

1. **Fix SQL syntax issues** for dbt-fusion compatibility
2. **Implement proper UUID generation** for entity IDs
3. **Create reference data tables** or update models to handle missing dependencies
4. **Update test syntax** in schema.yml files to use new dbt format
5. **Run comprehensive test suite** to validate all fixes