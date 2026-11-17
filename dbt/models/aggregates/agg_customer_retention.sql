WITH customer_orders AS (
    SELECT
        c.customer_unique_id,
        MIN(o.purchase_timestamp) AS first_purchase_timestamp,
        COUNT(DISTINCT o.order_id) AS order_count
    FROM {{ ref('fact_orders') }} o
    JOIN {{ ref('dim_customers') }} c ON o.customer_id = c.customer_id
    GROUP BY 1
),
cohorts AS (
    SELECT
        STRFTIME(first_purchase_timestamp, '%Y-%m') AS cohort_month,
        COUNT(*) AS customers,
        SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_customers
    FROM customer_orders
    GROUP BY 1
)
SELECT
    cohort_month,
    customers,
    repeat_customers,
    ROUND(repeat_customers * 100.0 / NULLIF(customers, 0), 1) AS repeat_rate_pct
FROM cohorts
ORDER BY 1
