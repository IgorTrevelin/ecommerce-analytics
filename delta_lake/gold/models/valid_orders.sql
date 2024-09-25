{{
    config(
        materialized='incremental',
        unique_key='order_id'
    )
}}
select

    *

from {{ source('silver', 'orders') }}

where status not in ('canceled', 'unavailable')

{% if is_incremental() %}

and purchase_timestamp >= (

    select

        coalesce(max(purchase_timestamp), '1900-01-01')

    from {{ this }}

)

{% endif %}