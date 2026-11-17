-- One row per bank account linked to a customer (bankInformations array).
with latest as (
    select customer_id, doc from {{ ref('int_customers_latest') }}
    where not is_deleted
)

select
    latest.customer_id,
    b.index                              as account_seq,
    b.value:accountNumber::string        as account_number,     -- PII: mask for analysts
    b.value:customerNumber::string       as bank_customer_number,
    b.value:exists::boolean              as account_exists,
    b.value:ownerVerified::boolean       as owner_verified,
    b.value:isPrimary::boolean           as is_primary
from latest,
     lateral flatten(input => latest.doc:bankInformations) b
