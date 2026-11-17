-- One row per customer address: the personalAddresses array flattened into rows.
with latest as (
    select customer_id, doc from {{ ref('int_customers_latest') }}
    where not is_deleted
),

addresses as (
    select
        latest.customer_id,
        a.index                                   as address_seq,
        upper(a.value:region::string)             as region_code,
        a.value:city::string                      as city,
        a.value:subcityZone::string               as subcity_zone,
        a.value:woreda::string                    as woreda,
        a.value:houseNumber::string               as house_number,
        {{ epoch_to_ts('a.value:createdDate') }}  as address_created_at
    from latest,
         lateral flatten(input => latest.doc:personalAddresses) a
)

select
    addresses.*,
    r.region_name
from addresses
left join {{ ref('region_codes') }} r
    on addresses.region_code = r.region_code
