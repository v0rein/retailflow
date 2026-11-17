SELECT
    c.customer_state,
    COUNT(f.order_id) AS delivered_orders_count,
    ROUND(AVG(f.delivery_days), 1) AS avg_delivery_days,
    ROUND(AVG(CASE WHEN f.is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_delivery_pct
FROM {{ ref('fact_orders') }} f
JOIN {{ ref('dim_customers') }} c ON f.customer_id = c.customer_id
WHERE f.order_status = 'delivered'
  AND f.delivery_days IS NOT NULL
GROUP BY 1
ORDER BY avg_delivery_days DESC
