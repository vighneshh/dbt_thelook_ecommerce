{{
  config(
    materialized='table'
  )
}}

with stg_order_items as (

    select
    id as order_item_id,
    order_id,
    user_id,
    product_id,
    inventory_item_id,
    status,
    created_at as order_item_created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price
    FROM `my-bi-project-406904.thelook_ecommerce.order_items`
),



stg_orders as (

SELECT
 order_id, 
 num_of_item 
 FROM `my-bi-project-406904.thelook_ecommerce.orders`
),


staging_order_items as (

    select
    stg_order_items.order_item_id,
    stg_order_items.order_id,
    stg_order_items.user_id,
    stg_order_items.product_id,
    stg_order_items.inventory_item_id,
    stg_order_items.status,
    stg_order_items.order_item_created_at,
    stg_order_items.shipped_at,
    stg_order_items.delivered_at,
    stg_order_items.returned_at,
    stg_order_items.sale_price

    from stg_order_items
    left join stg_orders 
    on stg_order_items.order_id = stg_orders.order_id
)

select * from staging_order_items

