with source_data as (
    select distinct Product as product_name,
        cast(PriceEach as FLOAT64) as price,
    from {{ ref('order_raw')}}
)

select 
    ROW_NUMBER() OVER () AS product_key,
    * 
from source_data