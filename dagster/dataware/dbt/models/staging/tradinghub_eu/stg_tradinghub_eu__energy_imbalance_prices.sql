-- Daily prices of THE's balancing regime, one row per gas day (EUR/MWh,
-- the unit is constant in the source and checked by a source test).
with source as (

    select * from {{ source('tradinghub_eu', 'pricesenergyimbalance') }}

)

select
    cast(gasday as date) as gas_day,
    -- volume-weighted average of the gas day's VTP exchange trades; the
    -- reference THE derives its imbalance prices from
    vtpdailyaverageprice as vtp_daily_average_price_eur_per_mwh,
    -- +-2 % band around it ("small adjustment")
    vtpdailyaveragepricesmalladjplus
        as vtp_small_adjustment_plus_eur_per_mwh,
    vtpdailyaveragepricesmalladjminus
        as vtp_small_adjustment_minus_eur_per_mwh,
    marginalpricesystembuy as marginal_price_system_buy_eur_per_mwh,
    marginalpricesystemsell as marginal_price_system_sell_eur_per_mwh,
    positiveenergyimbalanceprice as positive_imbalance_price_eur_per_mwh,
    negativeenergyimbalanceprice as negative_imbalance_price_eur_per_mwh
from source
