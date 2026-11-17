SELECT
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS TIMESTAMP) AS purchase_timestamp,
    CAST(order_approved_at AS TIMESTAMP) AS approved_timestamp,
    CAST(order_delivered_carrier_date AS TIMESTAMP) AS delivered_carrier_timestamp,
    CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_timestamp,
    CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_timestamp
FROM {{ source('silver', 'orders') }}
