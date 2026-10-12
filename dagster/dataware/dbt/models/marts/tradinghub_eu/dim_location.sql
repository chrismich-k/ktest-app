-- THE delivers no location master data; locations are derived from the
-- balancing actions and the long-term tender results. location_type and
-- exchange depend on location alone and are only known from the balancing
-- actions (the unique test on `location` fails the build if THE ever breaks
-- that); tender-only locations (e.g. storage facilities) leave them empty.
-- location_kind (storage | zone) comes from the hand-maintained seed
-- location_kinds, independent of location_type (how it is traded);
-- locations missing there get 'unknown' (warning test).
with balancing as (

    select distinct
        location,
        location_type,
        exchange
    from {{ ref('stg_tradinghub_eu__external_balancing_gas') }}

),

tender_only as (

    select distinct
        location,
        cast(null as varchar) as location_type,
        cast(null as varchar) as exchange
    from {{ ref('stg_tradinghub_eu__long_term_tenders') }}
    where location not in (select location from balancing)

),

locations as (

    select * from balancing
    union all
    select * from tender_only

)

select
    {{ dbt_utils.generate_surrogate_key(['locations.location']) }}
        as location_key,
    locations.location,
    locations.location_type,
    locations.exchange,
    coalesce(kinds.location_kind, 'unknown') as location_kind
from locations
left join {{ ref('location_kinds') }} as kinds
    on locations.location = kinds.location
