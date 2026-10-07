-- Anomalie connue du jeu Olist : une commande "delivered" sans aucun paiement.
-- Test conservé en severity=warn pour documenter l'anomalie sans bloquer le pipeline.
{{ config(severity='warn') }}

select
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp
from {{ ref('fct_orders') }} o
where o.payment_count = 0
