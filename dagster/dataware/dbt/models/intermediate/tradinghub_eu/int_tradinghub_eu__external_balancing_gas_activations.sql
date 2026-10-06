-- One row per balancing action, with a key that is unique by construction:
-- within each gas day (= Dagster partition), actions sharing all dimension
-- attributes are numbered by their measures. Every column is either in the
-- partition or in the ordering, so the numbering is deterministic; only
-- fully identical rows tie, and those are interchangeable.
{%- set dimension_columns = [
    'gas_day', 'activation_start_local', 'duration_h',
    'direction', 'gas_quality', 'product_type', 'call_type', 'term_type',
    'location', 'location_type', 'exchange', 'merit_order_rank',
] %}

with activations as (

    select * from {{ ref('stg_tradinghub_eu__external_balancing_gas') }}

),

numbered as (

    select
        *,
        row_number() over (
            partition by {{ dimension_columns | join(', ') }}
            order by
                commodity_charge_eur_per_mwh,
                hourly_quantity_mwh_per_h,
                number_of_lots,
                lot_size_mwh_per_h,
                daily_quantity_mwh,
                commodity_value_eur
        ) as activation_seq
    from activations

)

select
    {{ dbt_utils.generate_surrogate_key(
        dimension_columns + ['activation_seq']
    ) }} as activation_id,
    *
from numbered
