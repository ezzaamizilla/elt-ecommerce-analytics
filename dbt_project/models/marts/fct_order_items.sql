select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    i.customer_id,
    d.date_id as order_date_id,
    i.order_purchase_date,
    i.order_status,
    i.price,
    i.freight_value,
    i.item_total,
    i.product_category_english
from {{ ref('int_order_items') }} i
inner join {{ ref('dim_date') }} d
    on d.date_day = i.order_purchase_date
