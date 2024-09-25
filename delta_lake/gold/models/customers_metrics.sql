with customer_metrics as (
    select

        customer_id,
        max(date_trunc('day', purchase_timestamp)) as last_order_date,
        count(*) as num_orders_total

    from {{ ref('valid_orders') }}
    group by customer_id
)
, customer_order_purchase_total as (
    select
        vo.customer_id,
        sum(price + freight_value) as purchase_total,
        count(distinct product_id) as num_distinct_products
    from {{ source('silver', 'order_items') }} oi join {{ ref('valid_orders') }} vo
    on oi.order_id = vo.order_id
    group by customer_id
)
select
    c.customer_id,
    case
        when (current_timestamp - last_order_date) > INTERVAL '180' DAY then
            'inactive'
        else
            'active'
    end as customer_status,
    last_order_date,
    num_orders_total,
    purchase_total as purchase_total_value,
    num_distinct_products
from {{ source('silver', 'customers') }} c left join customer_metrics cm
    on c.customer_id = cm.customer_id
left join customer_order_purchase_total cpt
    on c.customer_id = cpt.customer_id