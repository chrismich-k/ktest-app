-- Spreading capacity prices over hours must neither lose nor add cost:
-- per location and tender product, the hourly costs have to add up to the
-- capacity prices of the lots.
with hourly as (

    select
        location_key,
        tender_product_key,
        sum(capacity_cost_eur) as capacity_cost_eur
    from {{ ref('fct_long_term_tender_capacity_cost_hourly') }}
    group by 1, 2

),

lots as (

    select
        location_key,
        tender_product_key,
        sum(capacity_cost_eur) as capacity_cost_eur
    from {{ ref('fct_long_term_tender_lots') }}
    group by 1, 2

)

select
    lots.location_key,
    lots.tender_product_key,
    lots.capacity_cost_eur as expected_eur,
    hourly.capacity_cost_eur as actual_eur
from lots
left join hourly
    on hourly.location_key = lots.location_key
    and hourly.tender_product_key = lots.tender_product_key
where hourly.location_key is null
    or abs(hourly.capacity_cost_eur - lots.capacity_cost_eur) > 0.01
