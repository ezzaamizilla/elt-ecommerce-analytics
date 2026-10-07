select
    order_id,
    payment_sequential,
    lower(trim(payment_type)) as payment_type,
    payment_installments,
    cast(payment_value as numeric(12, 2)) as payment_value
from {{ source('bronze', 'order_payments') }}
