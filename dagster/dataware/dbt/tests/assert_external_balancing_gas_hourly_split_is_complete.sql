-- Splitting into hours must neither lose nor add hours or value: per
-- action, the hourly rows have to add up to the original action.
with hourly as (

    select
        activation_id,
        count(*) as duration_h,
        sum(quantity_mwh) as quantity_mwh,
        sum(commodity_value_eur) as commodity_value_eur
    from {{ ref('fct_external_balancing_gas_hourly') }}
    group by activation_id

),

activations as (

    select
        activation_id,
        duration_h,
        daily_quantity_mwh as quantity_mwh,
        commodity_value_eur
    from {{ ref('int_tradinghub_eu__external_balancing_gas_activations') }}

)

select
    activations.activation_id,
    activations.duration_h as expected_duration_h,
    hourly.duration_h as actual_duration_h,
    activations.quantity_mwh as expected_quantity_mwh,
    hourly.quantity_mwh as actual_quantity_mwh,
    activations.commodity_value_eur as expected_value_eur,
    hourly.commodity_value_eur as actual_value_eur
from activations
left join hourly on hourly.activation_id = activations.activation_id
where hourly.activation_id is null
    or hourly.duration_h <> activations.duration_h
    or abs(hourly.quantity_mwh - activations.quantity_mwh) > 0.000001
    or abs(hourly.commodity_value_eur - activations.commodity_value_eur) > 0.000001
