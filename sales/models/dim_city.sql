with source_data as (
    select distinct PurchaseAddress
    from `SalesDataset.order_raw`
), 

transform_city as (
    SELECT SPLIT(PurchaseAddress, ', ')[SAFE_OFFSET(1)] AS city, 
        SPLIT(SPLIT(PurchaseAddress, ', ')[SAFE_OFFSET(2)], ' ')[SAFE_OFFSET(0)]  AS state_name,
        SPLIT(SPLIT(PurchaseAddress, ', ')[SAFE_OFFSET(2)], ' ')[SAFE_OFFSET(1)]  AS zipcode
    from source_data
),

match_dim_state as (
    select a.city,
        b.state_key,
        a.zipcode
    from transform_city a 
        join `SalesDataset.dim_state` b
            on a.state_name = b.state_name
),

clean_city as (
    select distinct * from match_dim_state
) 

select 
    ROW_NUMBER() OVER () AS city_key,
    state_key,
    city,
    zipcode
from clean_city
