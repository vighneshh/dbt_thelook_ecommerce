{{
  config(
    materialized='table'
  )
}}

with stg_users as (

    select
    id as user_id,
    first_name,
    last_name,
    email,
    age,
    gender,
    state,
    street_address,
    postal_code,
    city,
    country,
    latitude as user_latitude,
    longitude as user_longitude,
    traffic_source,
    created_at as users_created_at
    -- FROM `my-bi-project-406904.thelook_ecommerce.inventory_items`
    from {{ source('thelook_ecommerce', 'users') }}
),

stg_orders_inv_items as (
    select
    order_item_id,
    order_id,
    user_id,
    order_product_id,
    inventory_item_id,
    status,
    order_item_created_at,
    shipped_at,
    delivered_at,
    returned_at,
    sale_price,

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
    from {{ ref('stg_orders_inventory_items') }}

),

user_orders_items as (
    select
  stg_orders_inv_items.user_id,
    stg_users.first_name,
    stg_users.last_name,
    stg_users.email,
    stg_users.age,
    stg_users.gender,
    stg_users.state,
    stg_users.street_address,
    stg_users.postal_code,
    stg_users.city,
    stg_users.country,
     stg_users.user_latitude,
     stg_users.user_longitude,
    stg_users.traffic_source,
    stg_users.users_created_at,

    stg_orders_inv_items.order_item_id,
    stg_orders_inv_items.order_id,
    stg_orders_inv_items.order_product_id,
    stg_orders_inv_items.inventory_item_id,
    stg_orders_inv_items.status,
    stg_orders_inv_items.order_item_created_at,
    stg_orders_inv_items.shipped_at,
    stg_orders_inv_items.delivered_at,
    stg_orders_inv_items.returned_at,
    stg_orders_inv_items.sale_price,

    stg_orders_inv_items.product_id,
    stg_orders_inv_items.inventory_item_created_at,
    stg_orders_inv_items.sold_at,
    stg_orders_inv_items.cost,	
    stg_orders_inv_items.product_category,
    stg_orders_inv_items.product_name,
    stg_orders_inv_items.product_brand,
    stg_orders_inv_items.product_retail_price,
    stg_orders_inv_items.product_department,
    stg_orders_inv_items.product_sku,
    stg_orders_inv_items.product_distribution_center_id,
    stg_orders_inv_items.distribution_center_name
    from stg_users
    right join stg_orders_inv_items
    on stg_users.user_id = stg_orders_inv_items.user_id
)

select * from user_orders_items