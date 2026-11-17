-- One row per raw change event, with the top-level fields pulled out of the VARIANT document.
with source as (
    select * from {{ source('raw', 'customers_raw') }}
)

select
    customer_id,
    change_type,
    change_type = 'delete'                                   as is_delete_event,
    doc,
    doc:fullName::string                                     as full_name,
    doc:gender::string                                       as gender,
    doc:phone::string                                        as phone,
    doc:idType::string                                       as id_type,
    doc:idNumber::string                                     as id_number,
    doc:tinNumber::string                                    as tin_number,
    {{ epoch_to_ts('doc:birthDate') }}::date                 as birth_date,
    {{ epoch_to_ts('doc:createdDate') }}                     as source_created_at,
    {{ epoch_to_ts('doc:updatedDate') }}                     as source_updated_at,
    extracted_epoch,
    {{ epoch_to_ts('extracted_epoch') }}                     as extracted_at,
    _source_file,
    _source_row,
    _loaded_at
from source
