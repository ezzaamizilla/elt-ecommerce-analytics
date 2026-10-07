-- Aucun montant négatif ni nul dans les lignes de commande.

select
    order_id,
    order_item_id,
    price,
    freight_value
from {{ ref('fct_order_items') }}
where price <= 0
   or freight_value < 0
