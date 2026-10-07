select
    i.order_id,
    i.order_item_id,
    i.product_id,
    i.seller_id,
    o.customer_id,
    o.order_status,
    cast(o.order_purchase_timestamp as date) as order_purchase_date,
    i.price,
    i.freight_value,
    i.price + i.freight_value as item_total,
    p.product_category_english,
    p.product_category
from {{ ref('stg_order_items') }} i
inner join {{ ref('stg_orders') }} o on i.order_id = o.order_id
inner join {{ ref('stg_products') }} p on i.product_id = p.product_id
