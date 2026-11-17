{{
    config(
        materialized='incremental',
        unique_key='record_key',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

-- Current state of every customer = the latest change event per record_key (source + id).
-- Incremental: each run only reads events loaded since the last run (with a 1-hour lookback
-- for safety) and MERGEs them on record_key, so re-processing an event is harmless.

with events as (
    select * from {{ ref('stg_customer_events') }}
    {% if is_incremental() %}
    where _loaded_at > (select dateadd(hour, -1, max(_loaded_at)) from {{ this }})
    {% endif %}
)

select
    record_key,
    source_database,
    customer_id,
    is_delete_event as is_deleted,
    doc,
    full_name,
    gender,
    phone,
    id_type,
    id_number,
    tin_number,
    birth_date,
    source_created_at,
    source_updated_at,
    extracted_at,
    _loaded_at
from events
qualify row_number() over (
    partition by record_key
    order by extracted_epoch desc, _loaded_at desc, _source_row desc
) = 1
