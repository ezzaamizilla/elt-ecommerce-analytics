select
    d.year_month,
    d.month_number,
    d.year_number,
    count(distinct o.order_id) as order_count,
    count(o.item_count) as orders_with_items,
    sum(o.items_revenue) as revenue,
    sum(o.freight_total) as freight_revenue,
    sum(o.items_revenue) + sum(o.freight_total) as gross_revenue,
    avg(o.payment_total) as avg_payment_per_order,
    sum(case when o.is_delivered then 1 else 0 end) as delivered_orders
from {{ ref('fct_orders') }} o
inner join {{ ref('dim_date') }} d
    on d.date_id = o.order_date_id
group by
    d.year_month,
    d.month_number,
    d.year_number
