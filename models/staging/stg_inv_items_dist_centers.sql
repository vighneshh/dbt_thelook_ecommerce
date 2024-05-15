{{
  config(
    materialized='view'
  )
}}

with stg_inventory_items as (

    select
    id as inventory_item_id,
    product_id,
    created_at as inventory_item_created_at,
    sold_at,
    cost,	
    product_category,
    product_name,
    product_brand,
    product_retail_price,
    product_department,
    product_sku,
    product_distribution_center_id
    -- FROM `my-bi-project-406904.thelook_ecommerce.inventory_items`
    from {{ source('thelook_ecommerce', 'inventory_items') }}
),



stg_distribution_centers as (

    SELECT 
    name as distribution_center_name,
    id as distribution_center_id 
    -- FROM `my-bi-project-406904.thelook_ecommerce.distribution_centers`
    from {{ source('thelook_ecommerce', 'distribution_centers') }}

),







staging_inventory_items as (

    select
    stg_inventory_items.inventory_item_id,
    stg_inventory_items.product_id,
    stg_inventory_items.inventory_item_created_at,
    stg_inventory_items.sold_at,
    stg_inventory_items.cost,	
    stg_inventory_items.product_category,
    stg_inventory_items.product_name,
    stg_inventory_items.product_brand,
    stg_inventory_items.product_retail_price,
    stg_inventory_items.product_department,
    stg_inventory_items.product_sku,
    stg_inventory_items.product_distribution_center_id,
    stg_distribution_centers.distribution_center_name

    from stg_inventory_items
    left join stg_distribution_centers 
    on stg_inventory_items.product_distribution_center_id = stg_distribution_centers.distribution_center_id
)

select * from staging_inventory_items

