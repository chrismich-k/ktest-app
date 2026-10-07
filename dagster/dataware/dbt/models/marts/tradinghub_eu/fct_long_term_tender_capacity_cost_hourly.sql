-- Grain: one row per delivery hour, location and tender product.
-- Each lot's capacity price is spread evenly over the hours of its option
-- period and summed over all lots active in that hour. Lot-level detail
-- stays in fct_long_term_tender_lots; materialising lot x hour would be
-- ~28M rows. Hours are generated on the UTC instant (23h/25h days correct).
with lots as (

    select * from {{ ref('int_tradinghub_eu__long_term_tender_lots') }}

),

-- identical cost profiles only need to be expanded once
profiles as (

    select
        location,
        product,
        gas_quality,
        direction,
        calculation_type,
        capacity_type,
        period_start_utc,
        duration_h,
        count(*) as lots,
        sum(lot_quantity) as lot_quantity,
        sum(capacity_price_eur) / duration_h as capacity_cost_eur_per_h
    from lots
    group by 1, 2, 3, 4, 5, 6, 7, 8

),

hours as (

    select
        profiles.*,
        period_start_utc + hour_offset * interval '1' hour as hour_start_utc
    from profiles
    cross join unnest(sequence(0, duration_h - 1)) as t (hour_offset)

),

hourly as (

    select
        hour_start_utc,
        location,
        product,
        gas_quality,
        direction,
        calculation_type,
        capacity_type,
        sum(lots) as active_lots,
        sum(lot_quantity) as active_lot_quantity,
        sum(capacity_cost_eur_per_h) as capacity_cost_eur
    from hours
    group by 1, 2, 3, 4, 5, 6, 7

)

select
    -- dimension keys
    cast(format_datetime(
        at_timezone(hour_start_utc, 'Europe/Berlin'), 'yyyyMMdd'
    ) as integer) as calendar_date_key,
    -- gas day: local wall-clock time minus 6h (naive arithmetic, so the
    -- DST-change hour stays on the right gas day)
    cast(format_datetime(
        cast(at_timezone(hour_start_utc, 'Europe/Berlin') as timestamp)
        - interval '6' hour,
        'yyyyMMdd'
    ) as integer) as gas_day_date_key,
    {{ dbt_utils.generate_surrogate_key(['location']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key([
        'product', 'gas_quality', 'direction', 'calculation_type',
        'capacity_type',
    ]) }} as tender_product_key,

    -- delivery hour
    hour_start_utc,

    -- measures
    active_lots,
    active_lot_quantity,
    capacity_cost_eur

from hourly
