{{
  config(
    materialized='view',
    tags=['reference', 'staging']
  )
}}

/*
  Staging reference model for investor types
  Provides a table of investor type IDs and names for FK validation in tests.
  This derives from standardized investor types present in admin investors.
*/

with source as (
  select distinct standardized_investor_type as investor_type_name
  from {{ ref('stg_admin_investors') }}
  where standardized_investor_type is not null
),

typed as (
  select
    -- Generate a stable ID per investor type name
    cast(FARM_FINGERPRINT(investor_type_name) as varchar) as id,
    investor_type_name as name
  from source
)

select * from typed

