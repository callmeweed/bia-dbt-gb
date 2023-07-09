with source_data as (
    select distinct PurchaseAddress
    from {{ ref('order_raw')}}
), 

transform_state1 as(
    SELECT SPLIT(PurchaseAddress, ', ')[SAFE_OFFSET(2)] AS state_name1
    from source_data
),

transform_state2 as(
    SELECT distinct SPLIT(state_name1, ' ')[SAFE_OFFSET(0)] AS state_name
    from transform_state1
)


select 
    ROW_NUMBER() OVER () AS state_key,
    state_name
from transform_state2
