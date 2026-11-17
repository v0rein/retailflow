SELECT
    p.product_category_name_english AS category,
    ROUND(SUM(i.price), 2) AS category_revenue,
    ROUND(SUM(i.freight_value), 2) AS freight_revenue,
    COUNT(*) AS units_sold,
    COUNT(DISTINCT i.order_id) AS orders
FROM {{ ref('fact_order_items') }} i
JOIN {{ ref('dim_products') }} p ON i.product_id = p.product_id
JOIN {{ ref('fact_orders') }} f ON i.order_id = f.order_id
WHERE f.is_revenue_order
GROUP BY 1
ORDER BY category_revenue DESC
