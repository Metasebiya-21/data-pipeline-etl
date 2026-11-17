{{ config(materialized='table', schema='linkage') }}

-- Assign a master key to every record: the smallest record_key among itself and the
-- records it matched. (One hop only; full clusters need connected components - see docs.)

with matches as (
    select left_key, right_key from {{ ref('match_scores') }} where is_match
),

edges as (
    select left_key as record_key, right_key as other_key from matches
    union all
    select right_key, left_key from matches
)

select
    r.record_key,
    r.source_database,
    r.customer_id,
    least(r.record_key, coalesce(min(e.other_key), r.record_key)) as master_key,
    count(e.other_key)                                            as matched_records
from {{ ref('int_records_canonical') }} r
left join edges e on e.record_key = r.record_key
group by r.record_key, r.source_database, r.customer_id
