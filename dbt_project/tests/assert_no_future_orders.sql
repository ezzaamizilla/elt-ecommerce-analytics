-- Aucune commande ne peut être dans le futur.

select
    order_id,
    order_purchase_timestamp
from {{ ref('fct_orders') }}
where order_purchase_timestamp > current_timestamp
