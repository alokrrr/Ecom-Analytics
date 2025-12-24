# streamlit/app.py
import os
import streamlit as st
import pandas as pd
import requests
import altair as alt
from datetime import date, timedelta, datetime
from io import BytesIO

# ---------- Config ----------
API_BASE = os.getenv("API_BASE", "http://127.0.0.1:8000/kpi")  # read from env or use local
st.set_page_config(page_title="E-Commerce Analytics", layout="wide")

# ---------- Helpers ----------
@st.cache_data(ttl=60)
def fetch_json(path: str, params: dict = None):
    url = f"{API_BASE}{path}"
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"__error__": str(e)}

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")

def fmt_money(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "$0.00"

# ---------- Top bar ----------
st.title("E-Commerce Data Analytics & AI Insights")
st.caption("Built with Postgres (Supabase), FastAPI, Streamlit")

# ---------- Sidebar: Filters & controls (with unique keys) ----------
with st.sidebar:
    st.header("Filters & Connection")
    st.write("API base:", f"`{API_BASE}`")
    # quick backend test
    try:
        resp = requests.get(f"{API_BASE}/anomalies/health", timeout=2)
        backend_ok = resp.status_code == 200
    except Exception:
        backend_ok = False

    if not backend_ok:
        st.error("Backend unreachable. Start the FastAPI server (see terminal).")
        st.stop()

    # Categories from API
    cats_resp = fetch_json("/categories")
    if "__error__" in cats_resp:
        st.warning("Could not load categories from API.")
        categories_list = []
    else:
        categories_list = cats_resp

    selected_cats = st.multiselect(
        "Categories",
        options=categories_list,
        default=[],
        key="cat_filter"
    )

    # Price range slider
    price_range = st.slider(
        "Price range (min - max)",
        0.0,
        10000.0,
        (0.0, 1000.0),
        key="price_range_filter"
    )

    # Optional date range
    use_date_range = st.checkbox("Use date range", value=False, key="use_date_range")
    if use_date_range:
        default_end = date.today()
        default_start = default_end - timedelta(days=30)
        start_date = st.date_input("Start date", value=default_start, key="start_date_filter")
        end_date = st.date_input("End date", value=default_end, key="end_date_filter")
    else:
        start_date = None
        end_date = None

    # trend controls
    period = st.selectbox(
        "Trend period",
        ["monthly", "daily"],
        index=0,
        key="period_filter"
    )

    if period == "monthly":
        months = st.slider(
            "Months to show",
            1, 36, 12,
            key="months_filter"
        )
    else:
        months = st.slider(
            "Days to show (daily)",
            7, 90, 30,
            key="days_filter"
        )

    top_n = st.slider(
        "Number of top products",
        5, 100, 20,
        key="top_n_filter"
    )

st.markdown("---")

# ---------- Fetch overview (KPIs) ----------
overview = fetch_json("/overview")
if "__error__" in overview:
    st.error(f"API error - /overview: {overview['__error__']}")
    st.stop()

kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)
kpi_col1.metric("Total revenue (all-time)", fmt_money(overview.get("total_revenue", 0)))
kpi_col2.metric("Revenue (30d)", fmt_money(overview.get("revenue_30d", 0)),
                delta=f"{overview.get('pct_change_vs_prev_30d')}%")
kpi_col3.metric("MAU (30d)", f"{overview.get('mau_30d') or 0}")
kpi_col4.metric("Pct returning (30d)", f"{overview.get('pct_returning_30d') or 0}%")
kpi_col5.metric("Snapshot time", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M"))

# ---------- AI-Style KPI Insights ----------
st.subheader("📊 Smart KPI Insights")

def generate_kpi_insights(overview, top_products):
    insights = []
    pct = overview.get("pct_change_vs_prev_30d")
    if pct is not None:
        try:
            p = float(pct)
            if p > 0:
                insights.append(f"Revenue increased by **{p}%** in the last 30 days.")
            elif p < 0:
                insights.append(f"Revenue dropped by **{abs(p)}%**, consider analyzing slow-moving products.")
            else:
                insights.append("Revenue remained flat over the last 30 days.")
        except Exception:
            pass

    ret = overview.get("pct_returning_30d")
    if ret is not None:
        try:
            r = float(ret)
            if r < 10:
                insights.append(f"Returning customers are low (**{r}%**) — focus on retention.")
            elif r > 30:
                insights.append(f"Strong loyalty! **{r}%** of customers returned within 30 days.")
            else:
                insights.append(f"Returning customers: **{r}%**.")
        except Exception:
            pass

    insights.append(f"Active users in last 30 days: **{overview.get('mau_30d', 0)}**.")

    if isinstance(top_products, list) and len(top_products) > 0:
        tp = top_products[0]
        revenue = tp.get("revenue", 0)
        name = tp.get("name", "Top Product")
        try:
            insights.append(f"Top product **{name}** generated **${float(revenue):,.2f}** in revenue.")
        except Exception:
            insights.append(f"Top product **{name}** generated revenue (value unavailable).")
    return insights

# placeholder until top_products loaded
insights = generate_kpi_insights(overview, [])
for line in insights:
    st.write("• " + line)

st.markdown("---")

# ---------- Revenue Trend ----------
st.subheader("Revenue Trend")

trend_params = {}
if use_date_range and start_date and end_date:
    trend_params["start_date"] = start_date.isoformat()
    trend_params["end_date"] = end_date.isoformat()
else:
    trend_params["period"] = period
    trend_params["months"] = months

# filters
trend_params["min_price"] = price_range[0]
trend_params["max_price"] = price_range[1]
if selected_cats:
    trend_params["categories"] = ",".join(selected_cats)

trend = fetch_json("/revenue-trend", params=trend_params)
if "__error__" in trend:
    st.error(f"API error - /revenue-trend: {trend['__error__']}")
else:
    trend_df = pd.DataFrame(trend)
    if trend_df.empty:
        st.info("No revenue data for selected filters.")
    else:
        trend_df["period"] = pd.to_datetime(trend_df["period"])
        chart = (
            alt.Chart(trend_df)
            .mark_area(opacity=0.2)
            .encode(x=alt.X("period:T", title="Date"), y=alt.Y("revenue:Q", title="Revenue"))
        ) + (
            alt.Chart(trend_df)
            .mark_line(point=True)
            .encode(x="period:T", y="revenue:Q")
        )
        # updated param: width="stretch" instead of use_container_width
        st.altair_chart(chart.interactive(), width="stretch")
        st.download_button("Export trend CSV", data=df_to_csv_bytes(trend_df), file_name="revenue_trend.csv")

st.markdown("---")

# ---------- Revenue by Category ----------
st.subheader("Revenue by Category")
rb_params = {
    "min_price": price_range[0],
    "max_price": price_range[1]
}
if use_date_range and start_date and end_date:
    rb_params["start_date"] = start_date.isoformat()
    rb_params["end_date"] = end_date.isoformat()
if selected_cats:
    rb_params["categories"] = ",".join(selected_cats)

rev_by_cat = fetch_json("/revenue-by-category", params=rb_params)
if "__error__" in rev_by_cat:
    st.error(f"API error - /revenue-by-category: {rev_by_cat['__error__']}")
else:
    rbc_df = pd.DataFrame(rev_by_cat)
    if rbc_df.empty:
        st.info("No category revenue data.")
    else:
        bar = alt.Chart(rbc_df).mark_bar().encode(
            x=alt.X("revenue:Q", title="Revenue"),
            y=alt.Y("category:N", sort='-x', title="Category")
        )
        st.altair_chart(bar, width="stretch")
        st.download_button("Export category CSV", data=df_to_csv_bytes(rbc_df), file_name="revenue_by_category.csv")

st.markdown("---")

# ---------- Top Products ----------
st.subheader("Top Products")
tp_params = {"limit": top_n, "min_price": price_range[0], "max_price": price_range[1]}
if selected_cats:
    tp_params["categories"] = ",".join(selected_cats)

top_products = fetch_json("/products-by-category", params=tp_params)
if "__error__" in top_products:
    st.error(f"API error - /products-by-category: {top_products['__error__']}")
    top_products = []
else:
    tp_df = pd.DataFrame(top_products)
    if tp_df.empty:
        st.info("No product sales data.")
    else:
        tp_df["revenue"] = tp_df["revenue"].astype(float)
        tp_df["units_sold"] = tp_df["units_sold"].astype(int)
        c1, c2 = st.columns([2, 1])
        with c1:
            st.dataframe(tp_df[["product_id", "name", "price", "units_sold", "revenue"]], height=300)
        with c2:
            st.metric("Top product", tp_df.iloc[0]["name"])
            st.metric("Top revenue", f"${tp_df.iloc[0]['revenue']:.2f}")
        st.download_button("Export top products CSV", data=df_to_csv_bytes(tp_df), file_name="top_products.csv")

# update insights now that top_products loaded
st.write("")  # small spacer
insights = generate_kpi_insights(overview, top_products)
for line in insights:
    st.write("• " + line)

st.markdown("---")

# ---------- Customer Insights ----------
st.subheader("Customer Insights")
ci = fetch_json("/customer-insights")
if "__error__" in ci:
    st.error(f"API error - /customer-insights: {ci['__error__']}")
else:
    top_customers = pd.DataFrame(ci.get("top_customers", []))
    new_vs_repeat = ci.get("new_vs_repeat", {"new_customers": 0, "repeat_customers": 0, "pct_repeat": 0})
    st.metric("New customers (30d)", new_vs_repeat.get("new_customers", 0))
    st.metric("Repeat customers (30d)", new_vs_repeat.get("repeat_customers", 0))
    st.metric("Pct repeat (30d)", f"{new_vs_repeat.get('pct_repeat', 0)}%")
    if not top_customers.empty:
        st.write("Top Customers (by lifetime revenue)")
        top_customers["lifetime_revenue"] = top_customers["lifetime_revenue"].astype(float)
        st.dataframe(top_customers[["user_id", "email", "lifetime_revenue", "total_orders"]], height=300)
        st.download_button("Export top customers CSV", data=df_to_csv_bytes(top_customers), file_name="top_customers.csv")

st.markdown("---")

# ---------- Recent Reviews & Sentiment ----------
st.subheader("Recent Reviews & Sentiment")
reviews_params = {"limit": 200, "min_rating": 0}
reviews = fetch_json("/reviews", params=reviews_params)
if "__error__" in reviews:
    st.error(f"API error - /reviews: {reviews['__error__']}")
else:
    reviews_df = pd.DataFrame(reviews)
    if reviews_df.empty:
        st.info("No reviews available.")
    else:
        reviews_df["rating"] = reviews_df["rating"].astype("Int64")
        ordering = {"negative": 0, "neutral": 1, "positive": 2}
        reviews_df["sent_order"] = reviews_df["sentiment"].map(ordering).fillna(3)
        reviews_df = reviews_df.sort_values(["sent_order", "review_date"], ascending=[True, False])
        display_df = reviews_df[["review_date", "product_id", "user_id", "rating", "sentiment", "review_text"]]
        st.dataframe(display_df, height=360)
        st.download_button("Export reviews CSV", data=df_to_csv_bytes(reviews_df.drop(columns=["sent_order"])), file_name="recent_reviews.csv")

st.markdown("---")
st.caption("Tip: Use date-range for custom trend analysis. Reviews use a lightweight heuristic for sentiment; for production use an LLM or embeddings for more accuracy.")
st.markdown("---")

# ---------- Product Recommendations ----------
st.subheader("Product Recommendations")
prod_list = fetch_json("/products-list")
if "__error__" in prod_list or not prod_list:
    st.info("Product list unavailable for recommendations.")
else:
    prod_df = pd.DataFrame(prod_list)
    prod_options = prod_df.apply(lambda r: f"{r['product_id']} — {r['name']}", axis=1).tolist()
    selected = st.selectbox("Pick a product to get recommendations", prod_options, key="rec_prod_select")
    if selected:
        selected_product_id = int(selected.split(" — ")[0])
        method = st.radio("Method", ["co-purchase", "category"], key="rec_method")
        recs = fetch_json("/recommendations", params={"product_id": selected_product_id, "limit": 10, "method": method})
        if "__error__" in recs:
            st.error(f"API error - /recommendations: {recs['__error__']}")
        else:
            rec_df = pd.DataFrame(recs)
            if rec_df.empty:
                st.info("No recommendations found for this product.")
            else:
                if method == "co-purchase":
                    st.write("Top co-purchased items (support = proportion of orders containing base product):")
                    if "support" in rec_df:
                        rec_df["support_percent"] = rec_df["support"].apply(lambda x: f"{x*100:.2f}%")
                    st.dataframe(rec_df[["product_id", "name", "co_count", "co_revenue", "support"]], height=300)
                else:
                    st.write("Top items in same category (by revenue):")
                    st.dataframe(rec_df[["product_id", "name", "revenue"]], height=300)
                st.download_button("Export recommendations CSV", data=df_to_csv_bytes(rec_df), file_name="recommendations.csv")

st.markdown("---")

# ---------- Anomaly Detection Panel ----------
st.header("Revenue Anomaly Detection")
col1, col2, col3 = st.columns([2,1,1])
with col1:
    days_back = st.selectbox("Lookback window", [30, 60, 90, 180], index=2, key="anom_days")
    start_date_anom = st.date_input("Start date", value=date.today() - timedelta(days=days_back-1), key="anom_start")
    end_date_anom = st.date_input("End date", value=date.today(), key="anom_end")
with col2:
    method = st.selectbox("Method", ["zscore", "iqr"], key="anom_method")
    if method == "zscore":
        threshold = st.number_input("Z-threshold", min_value=0.5, value=3.0, step=0.5, key="anom_threshold")
    else:
        threshold = st.number_input("IQR multiplier", min_value=0.5, value=1.5, step=0.1, key="anom_threshold_iqr")
with col3:
    window = st.slider("Rolling window (days)", min_value=3, max_value=30, value=7, key="anom_window")

if st.button("Detect anomalies", key="anom_detect"):
    params = {
        "method": method,
        "window": window,
        "threshold": threshold,
        "start_date": start_date_anom.isoformat(),
        "end_date": end_date_anom.isoformat()
    }
    with st.spinner("Fetching data..."):
        r = requests.get(f"{API_BASE}/anomalies/detect", params=params)
    if r.status_code != 200:
        st.error(f"Error: {r.status_code} - {r.text}")
    else:
        data = r.json()
        series = pd.DataFrame(data.get("series", []))
        if not series.empty:
            series["day"] = pd.to_datetime(series["day"])
        anomalies = pd.DataFrame(data.get("anomalies", []))
        if not anomalies.empty:
            anomalies["day"] = pd.to_datetime(anomalies["day"])

        base = alt.Chart(series).mark_line().encode(
            x=alt.X('day:T', title='Date'),
            y=alt.Y('revenue:Q', title='Revenue')
        )

        points = alt.Chart(series).mark_circle(size=30).encode(
            x='day:T',
            y='revenue:Q',
            tooltip=['day:T', 'revenue:Q']
        )

        if not anomalies.empty:
            anom_points = alt.Chart(anomalies).mark_point(color='red', filled=True, size=100).encode(
                x='day:T',
                y='revenue:Q',
                tooltip=['day:T', alt.Tooltip('revenue:Q', title='Revenue'), alt.Tooltip('score:Q', title='Score'), alt.Tooltip('reason:N', title='Reason')]
            )
            st.altair_chart((base + points + anom_points).interactive(), width="stretch")
            st.markdown("### Detected anomalies")
            for _, row in anomalies.sort_values('day', ascending=False).iterrows():
                st.write(f"- **{row['day'].date()}** — revenue: {row['revenue']:.2f}, score: {row['score']:.2f} — {row['reason']}")
        else:
            st.altair_chart((base + points).interactive(), width="stretch")
            st.success("No anomalies detected for the chosen parameters.")
