-- Junk dimension: all occurring combinations of the low-cardinality
-- attributes of a long-term tender lot.
select distinct
    {{ dbt_utils.generate_surrogate_key([
        'product', 'gas_quality', 'direction', 'calculation_type',
        'capacity_type',
    ]) }} as tender_product_key,
    product,
    gas_quality,
    direction,
    calculation_type,
    capacity_type
from {{ ref('stg_tradinghub_eu__long_term_tenders') }}
