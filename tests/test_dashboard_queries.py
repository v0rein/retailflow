import duckdb
import pytest

from dashboard.setup_metabase import load_queries


@pytest.fixture()
def con():
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE agg_revenue_by_month "
        "(order_year_month VARCHAR, total_orders INTEGER, total_revenue DOUBLE, avg_order_value DOUBLE)"
    )
    connection.execute(
        "CREATE TABLE agg_delivery_performance "
        "(order_year_month VARCHAR, avg_delivery_days DOUBLE, delivered_orders_count INTEGER, late_delivery_pct DOUBLE)"
    )
    connection.execute(
        "CREATE TABLE agg_delivery_by_state "
        "(customer_state VARCHAR, delivered_orders_count INTEGER, avg_delivery_days DOUBLE, late_delivery_pct DOUBLE)"
    )
    connection.execute(
        "CREATE TABLE agg_revenue_by_category "
        "(category VARCHAR, category_revenue DOUBLE, units_sold INTEGER)"
    )
    connection.execute(
        "CREATE TABLE agg_seller_performance "
        "(seller_id VARCHAR, seller_state VARCHAR, seller_revenue DOUBLE, "
        "items_sold INTEGER, avg_review_score DOUBLE)"
    )
    connection.execute(
        "CREATE TABLE agg_customer_retention "
        "(cohort_month VARCHAR, customers BIGINT, repeat_customers HUGEINT, repeat_rate_pct DOUBLE)"
    )
    yield connection
    connection.close()


def test_queries_file_contains_all_6_business_queries():
    assert sorted(load_queries()) == ["Q1", "Q2", "Q3", "Q4", "Q5", "Q6"]


def test_every_dashboard_query_executes(con):
    for sql in (sql for _, sql in load_queries().values()):
        con.execute(sql)


def test_q4_splits_customers_into_one_time_and_repeat(con):
    con.execute("INSERT INTO agg_customer_retention VALUES ('2017-01', 10, 2, 20.0), ('2017-02', 5, 1, 20.0)")
    _, sql = load_queries()["Q4"]
    assert con.execute(sql).fetchall() == [("One-time", 12), ("Repeat", 3)]
