from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
DUCKDB_PATH = BASE_DIR / "data" / "order_intelligence.duckdb"

st.set_page_config(page_title="Order Intelligence CDC Pro", layout="wide")
st.title("Order Intelligence CDC Pro")
st.caption("Live analytics dashboard powered by Postgres CDC -> Kafka -> DuckDB")

if not DUCKDB_PATH.exists():
    st.error("DuckDB file not found yet. Start the consumer and let some CDC events arrive first.")
    st.stop()

con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

def table_exists(name: str) -> bool:
    return bool(con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()[0])

required = ["orders_current", "customers_current", "order_items_current", "products_current", "payments_current"]
missing = [name for name in required if not table_exists(name)]
if missing:
    st.error(f"Missing tables in DuckDB: {', '.join(missing)}")
    st.stop()

filters_df = con.execute("SELECT DISTINCT city FROM customers_current ORDER BY city").df()
status_df = con.execute("SELECT DISTINCT status FROM orders_current ORDER BY status").df()
category_df = con.execute("SELECT DISTINCT category FROM products_current ORDER BY category").df()

from datetime import date
import pandas as pd

raw_min_date, raw_max_date = con.execute(
    "SELECT MIN(order_ts)::DATE, MAX(order_ts)::DATE FROM orders_current"
).fetchone()

min_date = pd.to_datetime(raw_min_date).date() if raw_min_date else date.today()
max_date = pd.to_datetime(raw_max_date).date() if raw_max_date else date.today()

with st.sidebar:
    st.header("Filters")
    cities = st.multiselect("City", options=filters_df["city"].tolist(), default=filters_df["city"].tolist())
    statuses = st.multiselect("Order status", options=status_df["status"].tolist(), default=status_df["status"].tolist())
    categories = st.multiselect("Product category", options=category_df["category"].tolist(), default=category_df["category"].tolist())
    start_date, end_date = selected_dates = st.date_input("Date range", value=(min_date, max_date))

if isinstance(start_date, tuple):
    start_date, end_date = start_date

where_sql = """
WHERE c.city IN ({city_vals})
  AND o.status IN ({status_vals})
  AND p.category IN ({category_vals})
  AND CAST(o.order_ts AS DATE) BETWEEN ? AND ?
"""

def placeholders(items):
    return ",".join(["?"] * len(items)) if items else "NULL"

city_vals = placeholders(cities)
status_vals = placeholders(statuses)
category_vals = placeholders(categories)
params = list(cities) + list(statuses) + list(categories) + [start_date, end_date]
base_sql = where_sql.format(city_vals=city_vals, status_vals=status_vals, category_vals=category_vals)

kpi_query = f"""
SELECT
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(CASE WHEN o.status NOT IN ('canceled') THEN o.total_amount ELSE 0 END), 2) AS revenue,
    ROUND(AVG(CASE WHEN o.status NOT IN ('canceled') THEN o.total_amount END), 2) AS avg_order_value,
    ROUND(100.0 * SUM(CASE WHEN o.status = 'canceled' THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT o.order_id), 0), 2) AS cancel_rate,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN customer_orders.order_count > 1 THEN o.customer_id END) / NULLIF(COUNT(DISTINCT o.customer_id), 0), 2) AS repeat_customer_rate
FROM orders_current o
JOIN customers_current c ON o.customer_id = c.customer_id
JOIN order_items_current oi ON o.order_id = oi.order_id
JOIN products_current p ON oi.product_id = p.product_id
LEFT JOIN (
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders_current
    GROUP BY customer_id
) customer_orders ON o.customer_id = customer_orders.customer_id
{base_sql}
"""

kpis = con.execute(kpi_query, params).df().iloc[0]
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Orders", f"{int(kpis['total_orders'])}")
col2.metric("Revenue", f"${kpis['revenue']:,.2f}")
col3.metric("AOV", f"${kpis['avg_order_value']:,.2f}")
col4.metric("Cancel Rate", f"{kpis['cancel_rate']}%")
col5.metric("Repeat Customers", f"{kpis['repeat_customer_rate']}%")

trend_query = f"""
SELECT
    DATE_TRUNC('day', o.order_ts) AS order_day,
    COUNT(DISTINCT o.order_id) AS orders,
    ROUND(SUM(CASE WHEN o.status NOT IN ('canceled') THEN o.total_amount ELSE 0 END), 2) AS revenue
FROM orders_current o
JOIN customers_current c ON o.customer_id = c.customer_id
JOIN order_items_current oi ON o.order_id = oi.order_id
JOIN products_current p ON oi.product_id = p.product_id
{base_sql}
GROUP BY 1
ORDER BY 1
"""
trend_df = con.execute(trend_query, params).df()

left, right = st.columns((2, 1))
with left:
    fig = px.line(trend_df, x="order_day", y=["orders", "revenue"], markers=True, title="Orders and Revenue Trend")
    st.plotly_chart(fig, use_container_width=True)
with right:
    status_breakdown = con.execute(
        f"""
        SELECT o.status, COUNT(DISTINCT o.order_id) AS orders
        FROM orders_current o
        JOIN customers_current c ON o.customer_id = c.customer_id
        JOIN order_items_current oi ON o.order_id = oi.order_id
        JOIN products_current p ON oi.product_id = p.product_id
        {base_sql}
        GROUP BY 1
        ORDER BY 2 DESC
        """,
        params,
    ).df()
    fig2 = px.bar(status_breakdown, x="status", y="orders", title="Orders by Status")
    st.plotly_chart(fig2, use_container_width=True)

city_product_col, segment_col = st.columns((1.4, 1))
with city_product_col:
    city_df = con.execute(
        f"""
        SELECT c.city, COUNT(DISTINCT o.order_id) AS orders,
               ROUND(SUM(CASE WHEN o.status NOT IN ('canceled') THEN o.total_amount ELSE 0 END), 2) AS revenue
        FROM orders_current o
        JOIN customers_current c ON o.customer_id = c.customer_id
        JOIN order_items_current oi ON o.order_id = oi.order_id
        JOIN products_current p ON oi.product_id = p.product_id
        {base_sql}
        GROUP BY 1
        ORDER BY revenue DESC
        """,
        params,
    ).df()
    st.subheader("Revenue by City")
    st.dataframe(city_df, use_container_width=True, hide_index=True)

    top_products = con.execute(
        f"""
        SELECT p.product_name, p.category, SUM(oi.quantity) AS units_sold,
               ROUND(SUM(CASE WHEN o.status NOT IN ('canceled') THEN oi.line_amount ELSE 0 END), 2) AS revenue
        FROM order_items_current oi
        JOIN orders_current o ON oi.order_id = o.order_id
        JOIN products_current p ON oi.product_id = p.product_id
        JOIN customers_current c ON o.customer_id = c.customer_id
        {base_sql}
        GROUP BY 1,2
        ORDER BY revenue DESC
        LIMIT 10
        """,
        params,
    ).df()
    fig3 = px.bar(top_products, x="product_name", y="revenue", color="category", title="Top Products by Revenue")
    st.plotly_chart(fig3, use_container_width=True)

with segment_col:
    retention_df = con.execute(
        f"""
        WITH filtered_orders AS (
            SELECT DISTINCT o.order_id, o.customer_id, o.total_amount, c.loyalty_tier
            FROM orders_current o
            JOIN customers_current c ON o.customer_id = c.customer_id
            JOIN order_items_current oi ON o.order_id = oi.order_id
            JOIN products_current p ON oi.product_id = p.product_id
            {base_sql}
        )
        SELECT loyalty_tier,
               COUNT(DISTINCT customer_id) AS customers,
               COUNT(DISTINCT CASE WHEN order_count > 1 THEN customer_id END) AS repeat_customers,
               ROUND(AVG(total_amount), 2) AS avg_order_value
        FROM (
            SELECT fo.*, COUNT(*) OVER (PARTITION BY customer_id) AS order_count
            FROM filtered_orders fo
        ) x
        GROUP BY loyalty_tier
        ORDER BY customers DESC
        """,
        params,
    ).df()
    st.subheader("Customer Retention by Loyalty Tier")
    st.dataframe(retention_df, use_container_width=True, hide_index=True)

    payment_df = con.execute(
        """
        SELECT payment_status, COUNT(*) AS payment_count, ROUND(SUM(amount), 2) AS amount
        FROM payments_current
        GROUP BY 1
        ORDER BY amount DESC
        """
    ).df()
    fig4 = px.pie(payment_df, names="payment_status", values="amount", title="Payment Status Mix")
    st.plotly_chart(fig4, use_container_width=True)

st.subheader("Latest CDC Events")
if table_exists("raw_cdc_events"):
    cdc_df = con.execute(
        """
        SELECT entity, op, kafka_offset, TO_TIMESTAMP(event_ts_ms/1000.0) AS event_ts, processed_at
        FROM raw_cdc_events
        ORDER BY processed_at DESC
        LIMIT 30
        """
    ).df()
    st.dataframe(cdc_df, use_container_width=True, hide_index=True)
else:
    st.info("No raw CDC events yet. Start the consumer and trigger changes.")
