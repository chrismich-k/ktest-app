-- Junk dimension: all occurring combinations of the low-cardinality
-- attributes of a balancing action.
select distinct
    {{ dbt_utils.generate_surrogate_key([
        'direction', 'gas_quality', 'product_type', 'call_type', 'term_type',
    ]) }} as balancing_product_key,
    direction,
    gas_quality,
    product_type,
    call_type,
    term_type
from {{ ref('stg_tradinghub_eu__external_balancing_gas') }}
