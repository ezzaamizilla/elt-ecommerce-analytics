select
    order_id,
    count(*) as payment_count,
    sum(payment_value) as payment_total,
    max(payment_value) filter (where payment_value is not null) as payment_max,
    max(payment_installments) as payment_installments_max,
    (
        array_agg(payment_type order by payment_value desc nulls last)
    )[1] as main_payment_type
from {{ ref('stg_order_payments') }}
group by order_id
