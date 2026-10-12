-- Grain: one row per gas day. THE's daily balancing prices; the VTP daily
-- average price is the reference for the prices paid in balancing actions.
-- The current gas day is missing until THE publishes it after its end.
select
    cast(format_datetime(gas_day, 'yyyyMMdd') as integer) as gas_day_date_key,
    vtp_daily_average_price_eur_per_mwh,
    vtp_small_adjustment_plus_eur_per_mwh,
    vtp_small_adjustment_minus_eur_per_mwh,
    marginal_price_system_buy_eur_per_mwh,
    marginal_price_system_sell_eur_per_mwh,
    positive_imbalance_price_eur_per_mwh,
    negative_imbalance_price_eur_per_mwh
from {{ ref('stg_tradinghub_eu__energy_imbalance_prices') }}
