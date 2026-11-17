{{ config(materialized='table', schema='linkage') }}

-- Blocking: only compare records that share something cheap to check, instead of
-- comparing every record with every other record (88K x 100K = 8.8 billion pairs).
-- Pairs are across different sources, and each pair appears once (left source < right source).

with recs as (
    select * from {{ ref('int_records_canonical') }}
),

by_phone as (
    select l.record_key as left_key, r.record_key as right_key, 'same_phone' as blocking_rule
    from recs l join recs r
      on l.phone_9 = r.phone_9 and l.source_database < r.source_database
    where l.phone_9 is not null
),

by_name_dob as (
    select l.record_key, r.record_key, 'same_first_name_and_dob'
    from recs l join recs r
      on l.first_name = r.first_name and l.dob = r.dob and l.source_database < r.source_database
    where l.first_name is not null and l.dob is not null
),

by_id_number as (
    select l.record_key, r.record_key, 'same_id_number'
    from recs l join recs r
      on l.id_number = r.id_number and l.source_database < r.source_database
    where l.id_number is not null
),

all_pairs as (
    select * from by_phone
    union all select * from by_name_dob
    union all select * from by_id_number
)

select
    left_key,
    right_key,
    listagg(distinct blocking_rule, ',') within group (order by blocking_rule) as blocking_rules
from all_pairs
group by left_key, right_key
