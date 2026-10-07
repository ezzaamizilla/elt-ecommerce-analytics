select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    cast(price as numeric(12, 2)) as price,
    cast(freight_value as numeric(12, 2)) as freight_value
from {{ source('bronze', 'order_items') }}
