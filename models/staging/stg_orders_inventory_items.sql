{{
  config(
    materialized='view'
  )
}}

with stg_orders2 as (

    select 
    order_item_id,
    order_id,
    user_id,
    product_id as order_product_id,
    inventory_item_id,
    status,
    order_item_created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price
    from {{ ref('stg_order_items_orders') }}
  
),

stg_inv_items as (

    select 
    inventory_item_id,
    product_id,
    inventory_item_created_at,
    sold_at,
    cost,	
    product_category,
    product_name,
    product_brand,
    product_retail_price,
    product_department,
    product_sku,
    product_distribution_center_id,
    distribution_center_name
    from {{ ref('stg_inv_items_dist_centers') }}

),

stg_orders_inv_items as (

    select
    stg_orders2.order_item_id,
    stg_orders2.order_id,
    stg_orders2.user_id,
    stg_orders2.order_product_id,
    stg_orders2.inventory_item_id,
    stg_orders2.status,
    stg_orders2.order_item_created_at,
    stg_orders2.shipped_at,
    stg_orders2.delivered_at,
    stg_orders2.returned_at,
    stg_orders2.sale_price,

    stg_inv_items.product_id,
    stg_inv_items.inventory_item_created_at,
    stg_inv_items.sold_at,
    stg_inv_items.cost,	
    stg_inv_items.product_category,
    stg_inv_items.product_name,
    stg_inv_items.product_brand,
    stg_inv_items.product_retail_price,
    stg_inv_items.product_department,
    stg_inv_items.product_sku,
    stg_inv_items.product_distribution_center_id,
    stg_inv_items.distribution_center_name
    from stg_orders2
    left join stg_inv_items  
    on stg_orders2.inventory_item_id = stg_inv_items.inventory_item_id
)

select * from stg_orders_inv_items
