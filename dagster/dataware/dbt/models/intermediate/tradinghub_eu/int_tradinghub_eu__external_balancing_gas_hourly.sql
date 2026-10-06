-- One row per activation and delivery hour. Hours are generated on the UTC
-- instant, so 23h/25h gas days around DST changes need no special handling.
with activations as (

    select * from {{ ref('int_tradinghub_eu__external_balancing_gas_activations') }}

),

hours as (

    select
        activations.*,
        activation_start_utc + hour_offset * interval '1' hour as hour_start_utc
    from activations
    cross join unnest(sequence(0, duration_h - 1)) as t (hour_offset)

)

select
    activation_id,
    activation_seq,
    gas_day,
    hour_start_utc,
    at_timezone(hour_start_utc, 'Europe/Berlin') as hour_start_local,
    cast(at_timezone(hour_start_utc, 'Europe/Berlin') as date) as calendar_day,

    direction,
    gas_quality,
    merit_order_rank,
    product_type,
    call_type,
    term_type,
    location,
    location_type,
    exchange,

    -- THE's hourly quantity is rounded to 2 decimals (1000 MWh / 24h ->
    -- 41.67), and occasionally plain wrong; daily quantity and value are
    -- the leading figures, so distribute those evenly over the hours
    daily_quantity_mwh / duration_h as quantity_mwh,
    commodity_charge_eur_per_mwh,
    commodity_value_eur / duration_h as commodity_value_eur,

    -- original activation, for comparison
    activation_start_local as original_activation_start_local,
    duration_h as original_duration_h

from hours
