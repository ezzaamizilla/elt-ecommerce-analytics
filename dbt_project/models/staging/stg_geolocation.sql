select
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    lower(trim(geolocation_city)) as geolocation_city,
    upper(trim(geolocation_state)) as geolocation_state
from {{ source('bronze', 'geolocation') }}
