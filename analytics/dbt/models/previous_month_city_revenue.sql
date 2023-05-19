{{ config(materialized="view") }}

with
    previous_month_payments as (
        select o.order_id, sum(p.payment_value) order_value
        from {{ ref("previous_month_orders") }} o
        inner join payments p on p.order_id = o.order_id
        group by o.order_id
    ),
    previous_month_cutomer_spends as (
        select o.customer_id, sum(p.order_value) as spend
        from previous_month_payments p
        inner join orders o on p.order_id = o.order_id
        group by o.customer_id
    ),
    previous_month_city_revenue as (
        select c.customer_city, round(sum(s.spend), 2) as revenue
        from previous_month_cutomer_spends s
        inner join customers c on s.customer_id = c.customer_id
        group by c.customer_city
    )
select *
from previous_month_city_revenue
order by revenue desc
