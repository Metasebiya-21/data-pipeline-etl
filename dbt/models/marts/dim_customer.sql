-- Customer dimension (SCD Type 1: current values only). History of addresses is kept
-- separately in the customer_address_snapshot (SCD Type 2).
with latest as (
    select * from {{ ref('int_customers_latest') }}
    where not is_deleted
)

select
    customer_id,
    full_name,
    gender,
    phone,                                              -- PII: protect with a masking policy
    id_type,
    birth_date,
    doc:verification:phone::boolean                     as phone_verified,
    doc:userAccountInformation:preferredLanguage::string as preferred_language,
    doc:userAccountInformation:status::string           as account_status,
    doc:userAccountInformation:currentStage::string     as current_stage,
    doc:userAccountInformation:loanStatus::string       as loan_status,
    doc:userAccountInformation:loanProductCode::string  as loan_product_code,
    source_created_at                                   as created_at,
    source_updated_at                                   as updated_at
from latest
