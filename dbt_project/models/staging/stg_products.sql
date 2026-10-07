select
    p.product_id,
    coalesce(t.product_category_name_english, p.product_category_name, 'unknown') as product_category_english,
    coalesce(p.product_category_name, 'unknown') as product_category,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm
from {{ source('bronze', 'products') }} p
left join {{ source('bronze', 'category_translation') }} t
    on p.product_category_name = t.product_category_name
