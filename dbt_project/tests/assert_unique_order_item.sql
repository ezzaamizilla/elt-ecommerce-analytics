-- Unicité de la clé composite de la ligne de commande.

select
    order_id,
    order_item_id,
    count(*) as occurrences
from {{ ref('fct_order_items') }}
group by order_id, order_item_id
having count(*) > 1
