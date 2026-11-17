SELECT
    STRFTIME(purchase_timestamp, '%Y-%m') AS order_year_month,
    COUNT(order_id) AS total_orders,
    ROUND(SUM(total_revenue), 2) AS total_revenue,
    ROUND(AVG(total_revenue), 2) AS avg_order_value
FROM {{ ref('fact_orders') }}
WHERE is_revenue_order
GROUP BY 1
ORDER BY 1
