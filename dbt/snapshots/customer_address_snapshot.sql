{% snapshot customer_address_snapshot %}

{{
    config(
        unique_key='record_key',
        strategy='check',
        check_cols=['region', 'city', 'subcity_zone', 'woreda', 'house_number'],
        invalidate_hard_deletes=True
    )
}}

-- SCD Type 2 on each customer's primary (first) address.
-- When any checked column changes, dbt closes the old row (dbt_valid_to) and inserts a new
-- version, so we can answer "where did this customer live when the loan was issued?".
-- Customers that disappear (deleted in MongoDB) get their current row closed.

select
    record_key,
    source_database,
    customer_id,
    upper(doc:personalAddresses[0]:region::string)  as region,
    doc:personalAddresses[0]:city::string           as city,
    doc:personalAddresses[0]:subcityZone::string    as subcity_zone,
    doc:personalAddresses[0]:woreda::string         as woreda,
    doc:personalAddresses[0]:houseNumber::string    as house_number
from {{ ref('int_customers_latest') }}
where not is_deleted

{% endsnapshot %}
