select
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    order_count,
    first_order_date,
    last_order_date,
    lifetime_revenue
from {{ ref('int_customers_enriched') }}
