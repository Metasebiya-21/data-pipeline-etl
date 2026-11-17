-- Reconciliation: every non-deleted customer in the latest-state table must be in dim_customer.
-- Returns the missing customer_ids (the test fails if any rows come back).
select l.customer_id
from {{ ref('int_customers_latest') }} l
left join {{ ref('dim_customer') }} d on d.customer_id = l.customer_id
where not l.is_deleted
  and d.customer_id is null
