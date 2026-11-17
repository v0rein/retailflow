SELECT
    CONCAT(order_id, '_', CAST(order_item_id AS VARCHAR)) AS order_item_key,
    order_id,
    CAST(order_item_id AS INTEGER) AS order_item_id,
    product_id,
    seller_id,
    CAST(price AS DOUBLE) AS price,
    CAST(freight_value AS DOUBLE) AS freight_value,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_timestamp
FROM {{ source('silver', 'order_items') }}
