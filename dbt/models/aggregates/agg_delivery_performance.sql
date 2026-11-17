SELECT
    STRFTIME(purchase_timestamp, '%Y-%m') AS order_year_month,
    ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
    COUNT(order_id) AS delivered_orders_count,
    ROUND(AVG(CASE WHEN is_late THEN 1.0 ELSE 0.0 END) * 100, 1) AS late_delivery_pct
FROM {{ ref('fact_orders') }}
WHERE order_status = 'delivered'
  AND delivery_days IS NOT NULL
GROUP BY 1
ORDER BY 1
