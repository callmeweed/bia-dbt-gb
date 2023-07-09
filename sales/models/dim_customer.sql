with source_data as (
    select distinct PurchaseAddress
    from {{ ref('order_raw')}}
), 

transform_address as (
    SELECT SPLIT(PurchaseAddress, ', ')[SAFE_OFFSET(0)] AS street_address, 
        SPLIT(PurchaseAddress, ', ')[SAFE_OFFSET(1)] AS city, 
        SPLIT(SPLIT(PurchaseAddress, ', ')[SAFE_OFFSET(2)], ' ')[SAFE_OFFSET(1)]  AS zipcode
    from source_data
),

match_dim_city as (
    select a.street_address,
        b.city_key
    from transform_address a 
        join {{ ref('dim_city')}} b
            on a.city = b.city and a.zipcode = b.zipcode
),

clean_customer as (
    select  distinct * from match_dim_city
) 


select ROW_NUMBER() OVER () AS customer_key,
    * 
from clean_customer