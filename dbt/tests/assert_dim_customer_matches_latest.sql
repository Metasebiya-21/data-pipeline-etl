-- Reconciliation: every non-deleted customer in the latest-state table must be in dim_customer.
-- Returns the missing record_keys (the test fails if any rows come back).
select l.record_key
from {{ ref('int_customers_latest') }} l
left join {{ ref('dim_customer') }} d on d.record_key = l.record_key
where not l.is_deleted
  and d.record_key is null
