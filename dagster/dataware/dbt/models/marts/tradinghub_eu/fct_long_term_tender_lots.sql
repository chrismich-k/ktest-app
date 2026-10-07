-- Grain: one row per awarded long-term tender lot.
with lots as (

    select * from {{ ref('int_tradinghub_eu__long_term_tender_lots') }}

)

select
    -- dimension keys
    {{ dbt_utils.generate_surrogate_key(['location']) }} as location_key,
    {{ dbt_utils.generate_surrogate_key([
        'product', 'gas_quality', 'direction', 'calculation_type',
        'capacity_type',
    ]) }} as tender_product_key,
    cast(format_datetime(first_gas_day, 'yyyyMMdd') as integer)
        as first_gas_day_date_key,
    cast(format_datetime(last_gas_day, 'yyyyMMdd') as integer)
        as last_gas_day_date_key,

    -- degenerate dimensions
    lot_id,
    lot_seq,
    partition_month,

    -- option period
    period_start_utc,
    period_end_utc,
    duration_h,

    -- measures
    lot_quantity,
    call_days,
    capacity_price_eur as capacity_cost_eur,
    capacity_price_eur / duration_h as capacity_cost_eur_per_h

from lots
