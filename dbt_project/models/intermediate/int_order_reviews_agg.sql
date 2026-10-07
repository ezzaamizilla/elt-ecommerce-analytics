with deduplicated as (
    select
        *,
        row_number() over (
            partition by review_id, order_id
            order by review_answer_timestamp desc nulls last
        ) as dedup_rank
    from {{ ref('stg_order_reviews') }}
)

select
    order_id,
    count(*) as review_count,
    round(avg(review_score), 2) as review_avg_score,
    min(review_score) as review_min_score,
    max(review_score) as review_max_score,
    count(*) filter (where review_comment_message is not null) as review_comment_count
from deduplicated
where dedup_rank = 1
group by order_id
