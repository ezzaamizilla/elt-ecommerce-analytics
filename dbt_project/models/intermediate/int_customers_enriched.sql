select
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,
    count(o.order_id) as order_count,
    min(o.order_purchase_date) as first_order_date,
    max(o.order_purchase_date) as last_order_date,
    coalesce(sum(o.items_revenue), 0) as lifetime_revenue
from {{ ref('stg_customers') }} c
left join {{ ref('int_orders') }} o on c.customer_id = o.customer_id
group by
    c.customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state
