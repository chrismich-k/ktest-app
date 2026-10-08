-- Grain: one row per delivery hour, source, direction, product and location.
-- Balancing actions (EBG) and long-term tender capacity costs (LTO) in one
-- shape, with the sign already applied:
--   amount_eur > 0: cost for THE, < 0: revenue
--   EBG SystemBuy = cost, EBG SystemSell = revenue (a negative price turns
--   into a cost on its own), LTO capacity price = cost in both directions.
-- The energy price of called LTO options is part of the EBG rows already,
-- so only the capacity price comes from the tenders.
with balancing as (

    select
        'EBG' as source,
        product.direction,
        product.gas_quality,
        product.product_type || ' - ' || product.call_type || ' - '
        || product.term_type || ' (' || product.gas_quality || ')'
            as product_label,
        fact.location_key,
        fact.hour_start_utc,
        fact.calendar_date_key,
        fact.gas_day_date_key,
        case product.direction
            when 'SystemBuy' then fact.commodity_value_eur
            else -fact.commodity_value_eur
        end as amount_eur,
        fact.quantity_mwh
    from {{ ref('fct_external_balancing_gas_hourly') }} as fact
    inner join {{ ref('dim_balancing_product') }} as product
        on fact.balancing_product_key = product.balancing_product_key

),

tenders as (

    select
        'LTO' as source,
        product.direction,
        product.gas_quality,
        -- calculation/capacity type are 'n/a' for most products
        concat_ws(
            ' - ',
            product.product,
            nullif(product.calculation_type, 'n/a'),
            nullif(product.capacity_type, 'n/a')
        ) || ' (' || product.gas_quality || ')' as product_label,
        fact.location_key,
        fact.hour_start_utc,
        fact.calendar_date_key,
        fact.gas_day_date_key,
        fact.capacity_cost_eur as amount_eur,
        cast(null as double) as quantity_mwh
    from {{ ref('fct_long_term_tender_capacity_cost_hourly') }} as fact
    inner join {{ ref('dim_tender_product') }} as product
        on fact.tender_product_key = product.tender_product_key

),

costs as (

    select * from balancing
    union all
    select * from tenders

)

select
    costs.source,
    costs.direction,
    costs.gas_quality,
    costs.product_label,
    -- chart stack element (legend entry and colour): EBG per product and
    -- gas quality, LTO only per gas quality, both directions in one element
    case costs.source
        when 'LTO' then 'LTO Leistungspreis (' || costs.gas_quality || ')'
        else costs.product_label
    end as stack_label,
    -- stack side: EBG buy up, sell down (one chart series each, sharing
    -- the stack_label colour); LTO is a cost in both directions, so up
    case
        when costs.source = 'EBG' and costs.direction = 'SystemSell'
            then 'sell'
        else 'buy'
    end as stack_side,
    location.location,
    location.location_type,
    location.exchange,
    -- concat_ws skips the null attributes of tender-only locations
    concat_ws(
        ' - ', location.location, location.location_type, location.exchange
    ) as location_label,
    costs.hour_start_utc,
    calendar_day.date as calendar_date,
    gas_day.date as gas_day,
    sum(costs.amount_eur) as amount_eur,
    sum(costs.quantity_mwh) as quantity_mwh
from costs
inner join {{ ref('dim_location') }} as location
    on costs.location_key = location.location_key
inner join {{ ref('dim_date') }} as calendar_day
    on costs.calendar_date_key = calendar_day.date_key
inner join {{ ref('dim_date') }} as gas_day
    on costs.gas_day_date_key = gas_day.date_key
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
