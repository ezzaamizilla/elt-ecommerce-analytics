{{
    config(
        materialized='table'
    )
}}

with bounds as (
    select
        min(order_purchase_date) as min_date,
        max(order_purchase_date) as max_date
    from {{ ref('int_orders') }}
)

select
    to_char(d.day, 'YYYYMMDD')::int as date_id,
    d.day as date_day,
    extract(year from d.day)::int as year_number,
    extract(month from d.day)::int as month_number,
    extract(quarter from d.day)::int as quarter_number,
    extract(isodow from d.day)::int as iso_day_of_week,
    to_char(d.day, 'Day') as day_name,
    to_char(d.day, 'Month') as month_name,
    to_char(d.day, 'YYYY-MM') as year_month,
    extract(isodow from d.day) in (6, 7) as is_weekend
from bounds,
    lateral generate_series(bounds.min_date, bounds.max_date, interval '1 day') as d(day)
