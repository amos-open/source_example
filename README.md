# AMOS Source Example Package

## Overview

The `amos_source_example` package provides a comprehensive, realistic simulation of private markets data sources and their transformation into the AMOS canonical data model. This package serves as both a demonstration of the AMOS architecture and a reference implementation for building production source connectors.

## Purpose

This package demonstrates:
- **Realistic Source System Simulation**: Mirrors actual private markets systems including CRM, fund administration, portfolio management, and accounting platforms
- **Comprehensive Data Coverage**: Covers all major private markets data domains from fund setup to performance reporting
- **Multi-System Integration**: Shows how to integrate data from multiple source systems with different structures and formats
- **Best Practice Transformations**: Demonstrates staging and intermediate layer patterns for cleaning, standardizing, and preparing source data
- **Entity Resolution**: Provides robust cross-reference capabilities for maintaining data quality across systems
- **Performance Optimization**: Includes incremental processing patterns and performance optimization strategies

## Package Structure

```
amos_source_example/
├── dbt_project.yml              # Package configuration
├── README.md                    # This file
├── docs/                        # Documentation
│   ├── SOURCE_SYSTEMS.md        # Source system documentation
│   ├── DATA_LINEAGE.md          # Data lineage mapping
│   └── TRANSFORMATION_GUIDE.md  # Transformation best practices
├── seeds/                       # Seed data organized by source system
│   ├── crm_vendor/              # CRM system data
│   ├── fund_admin_vendor/       # Fund administration data
│   ├── portfolio_management_vendor/ # Portfolio management data
│   ├── accounting_vendor/       # Accounting system data
│   └── reference/               # Reference and cross-reference data
├── models/                      # dbt models
│   ├── staging/                 # Source-specific cleaning and standardization
│   │   ├── crm/                 # CRM staging models
│   │   ├── admin/               # Fund admin staging models
│   │   ├── pm/                  # Portfolio mgmt staging models
│   │   ├── accounting/          # Accounting staging models
│   │   └── reference/           # Reference data staging models
│   ├── intermediate/            # Cross-source preparation
│   │   ├── entities/            # Entity preparation models
│   │   ├── transactions/        # Transaction preparation models
│   │   ├── snapshots/           # Snapshot preparation models
│   │   └── relationships/       # Relationship preparation models
│   └── exports/                 # Final staging outputs for amos_core
├── macros/                      # Custom macros
│   ├── source_specific/         # Source system specific macros
│   └── transformations/         # Common transformation macros
└── tests/                       # Data quality tests
    ├── staging/                 # Staging layer tests
    ├── intermediate/            # Intermediate layer tests
    └── integration/             # Cross-layer integration tests
```

## Source Systems Simulated

### 1. CRM Vendor
Simulates customer relationship management systems like Salesforce, HubSpot, or Copper:
- Opportunity management and deal pipeline
- Company master data and industry classifications
- Contact management and relationship tracking

### 2. Fund Administration Vendor
Simulates fund administration platforms like eFront, Allvue, or Investran:
- Fund master data and terms
- Investor management and commitments
- Capital calls and distributions
- NAV calculations and reporting
- Fee and expense tracking

### 3. Portfolio Management Vendor
Simulates portfolio management systems:
- Investment master data and monitoring
- Company financial performance tracking
- Valuation history and methodologies
- KPI tracking and benchmarking

### 4. Accounting Vendor
Simulates accounting and ERP systems:
- Chart of accounts and financial structure
- Journal entries and transaction processing
- Bank transaction reconciliation
- Multi-entity and multi-currency support

### 5. Reference Data Management
Provides master data and cross-reference mappings:
- Currency, country, and industry reference data
- Historical exchange rates
- Cross-reference tables for entity resolution

## Configuration

### Package Variables

The package supports several configuration variables:

```yaml
vars:
  # Source system configuration
  active_source: 'amos_source_example'
  
  # Warehouse-specific settings
  raw_database: "RAW"
  staging_schema_suffix: "_stg_amos_source_example"
  
  # Data quality settings
  enable_data_quality_tests: true
  test_failure_threshold: 0.05
  
  # Performance settings
  enable_incremental_processing: true
  default_materialization: 'view'
  large_table_threshold: 1000000
```

### Multi-Warehouse Support

The package is designed to work across different data warehouse platforms:
- **Snowflake**: Primary development and testing platform
- **BigQuery**: Supported with platform-specific optimizations
- **Redshift**: Supported with appropriate configurations
- **Databricks**: Supported for Spark-based environments

## Getting Started

### Prerequisites

- dbt Core 1.0.0 or higher
- Access to a supported data warehouse
- amos_core package (for integration testing)

### Installation

1. Add the package to your `packages.yml`:
```yaml
packages:
  - git: "https://github.com/your-org/amos_source_example.git"
    revision: main
```

2. Install the package:
```bash
dbt deps
```

3. Configure your profile and variables as needed

4. Run the package:
```bash
# Load seed data
dbt seed --select amos_source_example

# Run staging models
dbt run --select amos_source_example.staging

# Run intermediate models
dbt run --select amos_source_example.intermediate

# Run export models
dbt run --select amos_source_example.exports

# Run tests
dbt test --select amos_source_example
```

## Integration with AMOS Core

This package is designed to integrate seamlessly with the `amos_core` package:

1. **Data Contracts**: Export models produce data in the exact format expected by amos_core canonical models
2. **Type Safety**: Enforces the same data types and constraints as the canonical layer
3. **Performance**: Supports the clustering and materialization strategies used by amos_core
4. **Testing**: Includes compatibility tests that verify integration with amos_core expectations

## Development Guidelines

### Adding New Source Systems

To add a new source system:

1. Create seed data in `seeds/{source_system_name}/`
2. Add staging models in `models/staging/{source_system_name}/`
3. Update intermediate models to incorporate the new source
4. Add appropriate tests and documentation
5. Update cross-reference tables for entity resolution

### Data Quality Standards

All models should follow these data quality standards:

- **Completeness**: Required fields must not be null
- **Validity**: Data must conform to expected formats and ranges
- **Consistency**: Cross-source data must align within defined tolerances
- **Timeliness**: Data freshness must meet defined SLAs
- **Accuracy**: Business rules and calculations must be correct

### Performance Considerations

- Use incremental materialization for large fact tables
- Implement appropriate clustering and partitioning strategies
- Optimize joins and aggregations for warehouse-specific performance
- Monitor query performance and resource usage

## Documentation

Comprehensive documentation is available in the `docs/` directory:

- **SOURCE_SYSTEMS.md**: Detailed explanation of each simulated source system
- **DATA_LINEAGE.md**: Complete mapping from source fields to canonical attributes
- **TRANSFORMATION_GUIDE.md**: Best practices for building source connectors

## Support and Contributing

For questions, issues, or contributions:

1. Check the documentation in the `docs/` directory
2. Review existing issues and discussions
3. Follow the contribution guidelines for code changes
4. Ensure all tests pass before submitting changes

## License

This package is part of the AMOS ecosystem and follows the same licensing terms as the core AMOS project.