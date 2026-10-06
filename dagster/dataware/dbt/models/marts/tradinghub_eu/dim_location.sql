-- THE delivers no location master data; locations are derived from the
-- activations. location_type and exchange depend on location alone (the
-- unique test on `location` fails the build if THE ever breaks that).
select distinct
    {{ dbt_utils.generate_surrogate_key(['location']) }} as location_key,
    location,
    location_type,
    exchange
from {{ ref('stg_tradinghub_eu__external_balancing_gas') }}
