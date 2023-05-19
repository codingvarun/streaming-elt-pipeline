{{ config(materialized="view") }}

with
    previous_month_items as (
        select product_id, count(*) as units
        from items
        where order_id in (select order_id from {{ref('previous_month_orders')}})
        group by product_id
    ),
    category_sales as (
        select p.product_category_name, count(*) as unit_sales
        from previous_month_items pmi
        inner join products p on pmi.product_id = p.product_id
        group by p.product_category_name
    ),
    category_sales_in_english as (
        select pc.product_category_name_english, unit_sales
        from category_sales cs
        inner join
            product_categories pc on cs.product_category_name = pc.product_category_name
    )
select *
from category_sales_in_english
order by unit_sales desc