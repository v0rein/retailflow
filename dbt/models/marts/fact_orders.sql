WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
items_agg AS (
    SELECT
        order_id,
        SUM(price) AS items_revenue,
        SUM(freight_value) AS total_freight,
        COUNT(*) AS total_items
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),
payments_agg AS (
    SELECT
        order_id,
        SUM(payment_value) AS payment_value,
        COUNT(*) AS payment_count
    FROM {{ ref('stg_payments') }}
    GROUP BY order_id
),
reviews_agg AS (
    SELECT
        order_id,
        AVG(review_score) AS review_score
    FROM {{ ref('stg_reviews') }}
    GROUP BY order_id
)
SELECT
    o.order_id,
    o.customer_id,
    o.order_status,
    o.purchase_timestamp,
    CAST(o.purchase_timestamp AS DATE) AS purchase_date_id,
    o.approved_timestamp,
    o.delivered_carrier_timestamp,
    o.delivered_timestamp,
    o.estimated_delivery_timestamp,
    COALESCE(i.items_revenue, 0.0) AS items_revenue,
    COALESCE(i.total_freight, 0.0) AS total_freight,
    COALESCE(i.items_revenue, 0.0) + COALESCE(i.total_freight, 0.0) AS total_revenue,
    COALESCE(i.total_items, 0) AS total_items,
    COALESCE(p.payment_value, 0.0) AS payment_value,
    COALESCE(p.payment_count, 0) AS payment_count,
    r.review_score,
    DATE_DIFF('day', o.purchase_timestamp, o.delivered_timestamp) AS delivery_days,
    -- The estimate is a date (always midnight), so compare dates: a same-day delivery is on time.
    CASE
        WHEN o.delivered_timestamp IS NOT NULL
             AND CAST(o.delivered_timestamp AS DATE) > CAST(o.estimated_delivery_timestamp AS DATE)
        THEN TRUE
        ELSE FALSE
    END AS is_late,
    -- Single revenue rule used by every revenue model and dashboard query.
    o.order_status NOT IN ('canceled', 'unavailable') AS is_revenue_order
FROM orders o
LEFT JOIN items_agg i ON o.order_id = i.order_id
LEFT JOIN payments_agg p ON o.order_id = p.order_id
LEFT JOIN reviews_agg r ON o.order_id = r.order_id
