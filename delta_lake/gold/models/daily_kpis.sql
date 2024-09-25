{{
    config(
        materialized='incremental',
        unique_key='date_ref'
    )
}}
with orders_with_order_date as (
    select
        date_trunc('day', purchase_timestamp) as order_date,
        order_id,
        customer_id
    from {{ ref('valid_orders') }}
    {% if is_incremental() %}
    where date_trunc('day', purchase_timestamp) >= (
        select
            coalesce(max(date_ref), '1900-01-01')
        from {{ this }}
    )
    {% endif %}
)
, order_items_with_values as (
    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        freight_value,
        (price + freight_value) as total_value
    from {{ source('silver', 'order_items') }}
    where order_id in (select distinct order_id from orders_with_order_date)
)
, order_items_joined as (
    select
        o.order_date,
        o.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        o.customer_id,
        oi.total_value,
        oi.freight_value
    from order_items_with_values oi left join orders_with_order_date o
        on oi.order_id = o.order_id
)
select

    order_date as date_ref,
    count(distinct order_id) as num_orders,
    count(distinct product_id) as num_distinct_products,
    count(distinct seller_id) as num_distinct_sellers,
    count(distinct customer_id) as num_distinct_customers,
    sum(total_value) as total_purchase_value,
    sum(freight_value) as total_freight_value

from order_items_joined
group by order_date
order by order_date asc