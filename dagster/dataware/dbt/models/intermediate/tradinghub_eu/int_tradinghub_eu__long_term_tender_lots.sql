-- One row per awarded lot, with a key. THE delivers identical rows for
-- separately awarded lots without an ID; within each set of identical rows
-- the lots are numbered (lot_seq). Every column is in the partition, so
-- only fully identical rows share a partition and they are interchangeable.
{%- set lot_columns = [
    'partition_month', 'product', 'gas_quality', 'direction',
    'calculation_type', 'capacity_type', 'location',
    'period_start_local', 'period_end_local', 'lot_quantity',
    'capacity_price_eur', 'energy_price', 'call_days', 'lead_time',
    'recovery_time', 'total_cost_eur',
] %}

with lots as (

    select * from {{ ref('stg_tradinghub_eu__long_term_tenders') }}

),

numbered as (

    select
        *,
        row_number() over (
            partition by {{ lot_columns | join(', ') }}
        ) as lot_seq
    from lots

)

select
    {{ dbt_utils.generate_surrogate_key(lot_columns + ['lot_seq']) }}
        as lot_id,
    *
from numbered
