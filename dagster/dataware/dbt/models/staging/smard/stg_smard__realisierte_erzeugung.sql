select
    "timestamp" as measured_at,
    region,
    data_id,
    value
from {{ source('smard', '1_1_quarterhour') }}
