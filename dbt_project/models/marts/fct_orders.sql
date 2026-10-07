select
    o.order_id,
    o.customer_id,
    d.date_id as order_date_id,
    o.order_purchase_timestamp,
    o.order_status,
    o.order_status = 'delivered' as is_delivered,
    o.item_count,
    o.items_revenue,
    o.freight_total,
    o.items_revenue + o.freight_total as order_gross_revenue,
    o.payment_count,
    o.payment_total,
    o.payment_installments_max,
    o.main_payment_type,
    o.review_count,
    o.review_avg_score,
    o.review_max_score,
    o.review_comment_count,
    o.delivery_delay_days,
    o.delivery_duration_days
from {{ ref('int_orders') }} o
inner join {{ ref('dim_date') }} d
    on d.date_day = o.order_purchase_date
