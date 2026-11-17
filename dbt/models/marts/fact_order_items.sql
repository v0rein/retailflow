SELECT
    order_item_key,
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    shipping_limit_timestamp,
    price + freight_value AS line_total
FROM {{ ref('stg_order_items') }}
