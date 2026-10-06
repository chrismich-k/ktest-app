with source as (

    select * from {{ source('tradinghub_eu', 'externalbalancinggas') }}

),

renamed as (

    select
        -- gas day (06:00 local -> 06:00 local next day)
        cast(gasday as date) as gas_day,

        -- activation period: source timestamps are naive Europe/Berlin
        -- local time; the end is computed on the UTC instant so that 23h/25h
        -- gas days around DST changes come out right
        timestampactivation as activation_start_local,
        at_timezone(
            with_timezone(timestampactivation, 'Europe/Berlin'), 'UTC'
        ) as activation_start_utc,
        at_timezone(
            with_timezone(timestampactivation, 'Europe/Berlin'), 'UTC'
        ) + duration * interval '1' hour as activation_end_utc,
        cast(duration as integer) as duration_h,

        -- product
        direction,
        gasquality as gas_quality,
        meritorderrank as merit_order_rank,
        producttype as product_type,
        calltype as call_type,
        termtype as term_type,
        location,
        locationtype as location_type,
        exchange,

        -- quantities and prices
        lotsize as lot_size_mwh_per_h,
        numberoflots as number_of_lots,
        hourlyquantity as hourly_quantity_mwh_per_h,
        dailyquantity as daily_quantity_mwh,
        commoditycharge as commodity_charge_eur_per_mwh,
        commodityvalue as commodity_value_eur

    from source

)

select * from renamed
