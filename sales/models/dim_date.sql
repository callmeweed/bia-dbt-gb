with source_data as (
    select distinct OrderDate
    from {{ ref('order_raw')}}
), 

transform_date as (
    select OrderDate,
        SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(0)] as month_no,
        SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(1)] as day_no,
        concat('20' ,SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(2)]) as year_no,

        SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(1)], ':') [SAFE_OFFSET(0)] as hour,

        {# PARSE_DATE('%m/%d/%Y', concat(SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(0)], '/', SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(1)], '/', concat('20' ,SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(2)]))) as date, #}
        EXTRACT(DAYOFWEEK  FROM PARSE_DATE('%m/%d/%Y', concat(SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(0)], '/', SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(1)], '/', concat('20' ,SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(2)]))) ) AS week_day_no
    from source_data
),

transform_weekdays as (
    select OrderDate,
        month_no,
        day_no,
        year_no,
        hour,
        week_day_no,
        case when week_day_no = 1 then 'Sunday'
            when week_day_no = 2 then 'Monday'
            when week_day_no = 3 then 'Tuesday'
            when week_day_no = 4 then 'Wednesday'
            when week_day_no = 5 then 'Thursday'
            when week_day_no = 6 then 'Friday'
            when week_day_no = 7 then 'Saturday'
        end as weekday_name,

        case when week_day_no = 1 then 'Y'
            when week_day_no = 7 then 'Y'
            else 'N'
        end as is_weekend
    from transform_date
)
{# weekday chủ nhật: 1 -> thứ 7: 7 #}
select 
    concat(SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(1)], SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(0)], concat('20' ,SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(0)], '/') [SAFE_OFFSET(2)]), SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(1)], ':') [SAFE_OFFSET(0)],  SPLIT(SPLIT(OrderDate, ' ')[SAFE_OFFSET(1)], ':') [SAFE_OFFSET(1)]) as date_key,
    * 
from transform_weekdays
