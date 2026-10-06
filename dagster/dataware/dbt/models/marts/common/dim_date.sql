-- Calendar dimension, one row per day. Used role-playing: as calendar day
-- and as gas day of a fact row.
with days as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="date_add('year', 2, current_date)",
    ) }}

)

select
    cast(format_datetime(date_day, 'yyyyMMdd') as integer) as date_key,
    cast(date_day as date) as date,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    format_datetime(date_day, 'MMMM') as month_name,
    week_of_year(date_day) as iso_week,
    year_of_week(date_day) as iso_year,
    day_of_week(date_day) as iso_day_of_week,
    format_datetime(date_day, 'EEEE') as day_name,
    day_of_week(date_day) >= 6 as is_weekend,
    -- German gas year (Gaswirtschaftsjahr) runs Oct 1 - Sep 30
    case
        when month(date_day) >= 10 then year(date_day)
        else year(date_day) - 1
    end as gas_year_start,
    case
        when month(date_day) >= 10
            then format('%d/%02d', year(date_day), (year(date_day) + 1) % 100)
        else format('%d/%02d', year(date_day) - 1, year(date_day) % 100)
    end as gas_year
from days
