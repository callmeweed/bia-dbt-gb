with source_data as (
    select *
    from {{ ref('fact_sales_order')}}
),

transform_quantity_revenue as (
    select
        date_key,
        product_key,
        sum(QuantityOrdered) as sum_of_quantity_ordered,
        sum(revenue) as sum_of_revenue,
    from source_data
    group by date_key, product_key
)

select count(*) from transform_quantity_revenue
