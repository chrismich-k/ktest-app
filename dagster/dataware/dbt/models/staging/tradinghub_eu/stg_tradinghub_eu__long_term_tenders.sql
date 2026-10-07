with source as (

    select * from {{ source('tradinghub_eu', 'resultslongtermtenders') }}

),

renamed as (

    select
        -- product
        produkt as product,
        gasart as gas_quality,
        flussrichtung as direction,
        berechnungsart as calculation_type,
        kapazitaetstyp as capacity_type,
        ort as location,

        -- option period: source timestamps are naive Europe/Berlin local
        -- time (gas-day aligned, 06:00 -> 06:00); duration is counted on the
        -- UTC instants so months with a DST change get 743/745 hours
        start as period_start_local,
        ende as period_end_local,
        at_timezone(with_timezone(start, 'Europe/Berlin'), 'UTC')
            as period_start_utc,
        at_timezone(with_timezone(ende, 'Europe/Berlin'), 'UTC')
            as period_end_utc,
        date_diff(
            'hour',
            with_timezone(start, 'Europe/Berlin'),
            with_timezone(ende, 'Europe/Berlin')
        ) as duration_h,
        cast(start as date) as first_gas_day,
        date_add('day', -1, cast(ende as date)) as last_gas_day,

        -- THE field `los`; unit undocumented
        los as lot_quantity,
        -- capacity price for the whole option period (€)
        leistungspreis as capacity_price_eur,
        -- energy price; no longer used by THE in recent years
        arbeitspreis as energy_price,
        anzahl_abruftage as call_days,
        vorlaufzeit as lead_time,
        recovery_zeit as recovery_time,
        gesamt_kosten as total_cost_eur,

        dg_monthly_partition as partition_month

    from source

)

select * from renamed
