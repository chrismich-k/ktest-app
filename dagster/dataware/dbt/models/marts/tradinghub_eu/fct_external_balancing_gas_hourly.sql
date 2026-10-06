-- Grain: one row per balancing action and delivery hour.
-- Daily figures are aggregated from this table on demand.
with hourly as (

    select * from {{ ref('int_tradinghub_eu__external_balancing_gas_hourly') }}

)

select
    -- dimension keys
    cast(format_datetime(calendar_day, 'yyyyMMdd') as integer)
        as calendar_date_key,
    cast(format_datetime(gas_day, 'yyyyMMdd') as integer) as gas_day_date_key,
    {{ dbt_utils.generate_surrogate_key(['location']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key([
        'direction', 'gas_quality', 'product_type', 'call_type', 'term_type',
    ]) }} as balancing_product_key,

    -- degenerate dimensions
    activation_id,
    activation_seq,
    merit_order_rank,
    original_activation_start_local,
    original_duration_h,

    -- delivery hour
    hour_start_utc,
    hour_start_local,

    -- measures
    quantity_mwh,
    commodity_charge_eur_per_mwh,
    commodity_value_eur

from hourly
