-- One row per business owned by a customer (businesses array).
with latest as (
    select record_key, source_database, customer_id, doc from {{ ref('int_customers_latest') }}
    where not is_deleted
)

select
    latest.source_database || ':' || b.value:businessId::string as business_key,
    b.value:businessId::string                            as business_id,
    latest.record_key,
    latest.source_database,
    latest.customer_id,
    b.value:businessName::string                          as business_name,
    b.value:businessSector::string                        as business_sector,
    b.value:lineOfBusiness::string                        as line_of_business,
    b.value:formOfBusiness::string                        as form_of_business,
    b.value:associationType::string                       as association_type,
    {{ epoch_to_ts('b.value:businessRegisteredDate') }}::date as registered_date,
    b.value:numberOfEmployees::number                     as number_of_employees,
    b.value:startingCapital::number(18,2)                 as starting_capital,
    b.value:currentCapital::number(18,2)                  as current_capital,
    b.value:annualSalesIncome::number(18,2)               as annual_sales_income,
    b.value:annualProfit::number(18,2)                    as annual_profit
from latest,
     lateral flatten(input => latest.doc:businesses) b
where b.value:businessId is not null
