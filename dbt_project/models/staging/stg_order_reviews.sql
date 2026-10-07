select
    review_id,
    order_id,
    review_score,
    nullif(trim(review_comment_title), '') as review_comment_title,
    nullif(trim(review_comment_message), '') as review_comment_message,
    review_creation_date,
    review_answer_timestamp
from {{ source('bronze', 'order_reviews') }}
