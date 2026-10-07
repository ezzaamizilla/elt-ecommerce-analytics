-- Reconciliation : le CA mensuel agrégé doit égaler la somme des lignes de commande.

with revenue_month as (
    select sum(revenue) as total from {{ ref('fct_revenue') }}
),
order_items as (
    select sum(price) as total from {{ ref('fct_order_items') }}
)
select
    r.total as revenue_month_total,
    i.total as order_items_total
from revenue_month r
cross join order_items i
where round(coalesce(r.total, 0), 2) <> round(coalesce(i.total, 0), 2)
