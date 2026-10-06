-- THE's hourly quantity is daily quantity / duration rounded to 2 decimals,
-- so hourly x duration may deviate from daily by at most duration x 0.005.
-- Anything beyond that is a data error at THE (e.g. gas day 2024-11-14:
-- 24 MWh daily, but 0.04 MWh/h). Our models use the daily quantity, so this
-- only warns.
{{ config(severity='warn') }}

select
    gas_day,
    activation_start_local,
    location,
    direction,
    duration_h,
    hourly_quantity_mwh_per_h,
    daily_quantity_mwh,
    hourly_quantity_mwh_per_h * duration_h as hourly_x_duration_mwh
from {{ ref('stg_tradinghub_eu__external_balancing_gas') }}
where abs(hourly_quantity_mwh_per_h * duration_h - daily_quantity_mwh)
    > duration_h * 0.005 + 0.000001
