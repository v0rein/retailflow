WITH revenue_items AS (
    SELECT i.*, f.review_score
    FROM {{ ref('fact_order_items') }} i
    JOIN {{ ref('fact_orders') }} f ON i.order_id = f.order_id
    WHERE f.is_revenue_order
),
seller_sales AS (
    SELECT
        seller_id,
        ROUND(SUM(price), 2) AS seller_revenue,
        ROUND(SUM(freight_value), 2) AS freight_revenue,
        COUNT(*) AS items_sold,
        COUNT(DISTINCT order_id) AS orders
    FROM revenue_items
    GROUP BY 1
),
-- One row per seller and order, so an order with 3 items counts its review once.
seller_order_reviews AS (
    SELECT DISTINCT seller_id, order_id, review_score
    FROM revenue_items
),
seller_reviews AS (
    SELECT seller_id, ROUND(AVG(review_score), 2) AS avg_review_score
    FROM seller_order_reviews
    GROUP BY 1
)
SELECT
    s.seller_id,
    s.seller_state,
    ss.seller_revenue,
    ss.freight_revenue,
    ss.items_sold,
    ss.orders,
    r.avg_review_score
FROM seller_sales ss
JOIN {{ ref('dim_sellers') }} s ON ss.seller_id = s.seller_id
JOIN seller_reviews r ON ss.seller_id = r.seller_id
ORDER BY seller_revenue DESC
