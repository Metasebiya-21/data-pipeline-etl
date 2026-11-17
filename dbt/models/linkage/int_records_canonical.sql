{{ config(materialized='table', schema='linkage') }}

-- Canonical (normalised) customer records from all sources: the input to record linkage.
-- Same idea as the Postgres records_er_canonical view, written for Snowflake VARIANT.

with base as (
    select
        record_key,
        source_database,
        customer_id,
        {{ clean_text('full_name') }}                                                     as full_name,
        upper({{ clean_text('gender') }})                                                 as gender,
        upper(regexp_replace({{ clean_text('id_number') }}, '\\s', ''))                   as id_number,
        birth_date,
        {{ clean_text('phone') }}                                                         as phone,
        {{ clean_text("doc:bankInformations[0]:additionalInformation:mobile::string") }}  as bank_mobile,
        upper({{ clean_text("doc:personalAddresses[0]:region::string") }})                as region,
        {{ clean_text("doc:personalAddresses[0]:woreda::string") }}                       as woreda,
        coalesce(doc:verification:phone::boolean, false)                                  as phone_verified,
        coalesce(doc:bankInformations[0]:ownerVerified::boolean, false)                   as bank_owner_verified
    from {{ ref('int_customers_latest') }}
    where not is_deleted
),

canon as (
    select
        *,
        lower(regexp_replace(full_name, '\\s+', ' '))                                     as full_name_norm,
        nullif(split_part(lower(regexp_replace(full_name, '\\s+', ' ')), ' ', 1), '')     as first_name,
        to_char(birth_date, 'YYYY-MM-DD')                                                 as dob,
        nullif(right(regexp_replace(coalesce(phone, ''), '\\D', ''), 9), '')              as phone_9,
        nullif(right(regexp_replace(coalesce(bank_mobile, ''), '\\D', ''), 9), '')        as bank_mobile_9
    from base
)

select
    *,
    coalesce(phone_9 = bank_mobile_9, false)                                              as phone_matches_bank_mobile,
    phone_verified and bank_owner_verified and coalesce(phone_9 = bank_mobile_9, false)    as strong_identity_verified
from canon
