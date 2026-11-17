-- RetailFlow Gold Layer Business Queries (DuckDB dialect)
-- Connect Metabase to /data/lakehouse/retailflow.duckdb and run these as native queries.

-- Q1: Monthly Revenue Trend
SELECT
    order_year_month,
    total_orders,
    total_revenue,
    avg_order_value
FROM agg_revenue_by_month
-- Drop the sparse months where the dataset window starts/ends (2016, 2018-09) so they don't read as crashes.
WHERE total_orders >= 500
ORDER BY order_year_month;

-- Q2: Top Product Categories by Revenue
SELECT
    category,
    category_revenue,
    units_sold
FROM agg_revenue_by_category
ORDER BY category_revenue DESC
LIMIT 10;

-- Q3: Delivery Performance by Month
SELECT
    order_year_month,
    avg_delivery_days,
    delivered_orders_count,
    late_delivery_pct
FROM agg_delivery_performance
ORDER BY order_year_month;

-- Q4: Customer Repeat Purchase Cohorts
SELECT 'One-time' AS buyer_type, CAST(SUM(customers - repeat_customers) AS BIGINT) AS customer_count
FROM agg_customer_retention
UNION ALL
SELECT 'Repeat', CAST(SUM(repeat_customers) AS BIGINT)
FROM agg_customer_retention
ORDER BY 1;

-- Q5: Top Sellers Performance
SELECT
    seller_id,
    seller_state,
    seller_revenue,
    items_sold,
    avg_review_score
FROM agg_seller_performance
ORDER BY seller_revenue DESC
LIMIT 10;

-- Q6: Delivery Performance by State (worst first)
SELECT
    customer_state,
    delivered_orders_count,
    avg_delivery_days,
    late_delivery_pct
FROM agg_delivery_by_state
ORDER BY avg_delivery_days DESC, late_delivery_pct DESC;
