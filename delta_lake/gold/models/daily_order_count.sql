
with order_with_purchase_date as (

    select

        date(purchase_timestamp) as order_date

    from {{ source('silver', 'orders') }}

)

, daily_order_count as (

    select

        order_date,
        count(*) as order_count

    from order_with_purchase_date
    group by order_date
    order by order_date asc
)

select *

from daily_order_count