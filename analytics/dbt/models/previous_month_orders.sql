{{ config(materialized="view") }}

select order_id,customer_id
from orders
where
    datediff('day', order_delivered_customer_date, current_date()) < 31
    and datediff('day', order_delivered_customer_date, current_date()) > -1
