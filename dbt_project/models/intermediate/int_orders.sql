select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    cast(o.order_purchase_timestamp as date) as order_purchase_date,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    coalesce(it.item_count, 0) as item_count,
    coalesce(it.items_revenue, 0) as items_revenue,
    coalesce(it.freight_total, 0) as freight_total,

    coalesce(p.payment_count, 0) as payment_count,
    coalesce(p.payment_total, 0) as payment_total,
    p.payment_installments_max,
    p.main_payment_type,

    r.review_count,
    r.review_avg_score,
    r.review_max_score,
    r.review_comment_count,

    case
        when o.order_status = 'delivered' and o.order_delivered_customer_date is not null
        then (cast(o.order_delivered_customer_date as date)
              - cast(o.order_estimated_delivery_date as date))
    end as delivery_delay_days,

    case
        when o.order_status = 'delivered' and o.order_delivered_customer_date is not null
        then (cast(o.order_delivered_customer_date as date)
              - cast(o.order_purchase_timestamp as date))
    end as delivery_duration_days

from {{ ref('stg_orders') }} o
left join {{ ref('int_order_payments_agg') }} p using (order_id)
left join {{ ref('int_order_reviews_agg') }} r using (order_id)
left join (
    select
        order_id,
        count(*) as item_count,
        sum(price) as items_revenue,
        sum(freight_value) as freight_total
    from {{ ref('int_order_items') }}
    group by order_id
) it using (order_id)
