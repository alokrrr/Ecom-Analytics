# backend/api/kpi.py
from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from backend.db import  get_async_session

router = APIRouter()


# -----------------------
# /kpi/overview
# -----------------------
@router.get("/overview")
async def kpi_overview(db: AsyncSession = Depends( get_async_session)):
    KPI_OVERVIEW_SQL = text("""
    WITH total_rev AS (
      SELECT SUM(oi.quantity * oi.unit_price) AS total_revenue
      FROM ecom.orders o
      JOIN ecom.order_items oi ON oi.order_id = o.order_id
      WHERE o.status = 'completed'
    ),
    rev_30d AS (
      SELECT SUM(oi.quantity * oi.unit_price) AS revenue_30d
      FROM ecom.orders o
      JOIN ecom.order_items oi ON oi.order_id = o.order_id
      WHERE o.status = 'completed'
        AND o.order_date::timestamp >= now()::date - INTERVAL '29 days'
    ),
    rev_30_60d AS (
      SELECT SUM(oi.quantity * oi.unit_price) AS revenue_prev_30d
      FROM ecom.orders o
      JOIN ecom.order_items oi ON oi.order_id = o.order_id
      WHERE o.status = 'completed'
        AND o.order_date::timestamp >= now()::date - INTERVAL '59 days'
        AND o.order_date::timestamp <  now()::date - INTERVAL '30 days'
    ),
    mau_30d AS (
      SELECT COUNT(DISTINCT user_id) AS mau_30d
      FROM ecom.orders
      WHERE status = 'completed'
        AND order_date::timestamp >= now()::date - INTERVAL '29 days'
    ),
    returning_pct_30d AS (
      SELECT
        100.0 * SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END)::numeric 
          / NULLIF(COUNT(*)::numeric, 0) AS pct_returning
      FROM (
        SELECT user_id, COUNT(DISTINCT order_id) AS cnt
        FROM ecom.orders
        WHERE status = 'completed'
          AND order_date::timestamp >= now()::date - INTERVAL '29 days'
        GROUP BY user_id
      ) t
    )
    SELECT
      tr.total_revenue,
      r30.revenue_30d,
      ROUND(
          (
            (r30.revenue_30d - COALESCE(rp.revenue_prev_30d, 0)) 
            / NULLIF(rp.revenue_prev_30d, 0)
          )::numeric
        , 2
      ) AS pct_change_vs_prev_30d,
      m.mau_30d,
      ROUND(rpct.pct_returning, 2) AS pct_returning_30d
    FROM total_rev tr
    CROSS JOIN rev_30d r30
    CROSS JOIN rev_30_60d rp
    CROSS JOIN mau_30d m
    CROSS JOIN returning_pct_30d rpct;
    """)
    try:
        result = await db.execute(KPI_OVERVIEW_SQL)
        row = result.fetchone()
        if not row:
            return {
                "total_revenue": 0,
                "revenue_30d": 0,
                "pct_change_vs_prev_30d": None,
                "mau_30d": 0,
                "pct_returning_30d": 0
            }

        mapping = getattr(row, "_mapping", None)
        def get_val(key, idx):
            if mapping is not None and key in mapping:
                return mapping[key]
            try:
                return row[idx]
            except Exception:
                return None

        total_revenue = get_val("total_revenue", 0)
        revenue_30d = get_val("revenue_30d", 1)
        pct_change = get_val("pct_change_vs_prev_30d", 2)
        mau_30d = get_val("mau_30d", 3)
        pct_returning = get_val("pct_returning_30d", 4)

        def to_float(v):
            if v is None:
                return None
            try:
                return float(v)
            except Exception:
                return v

        return {
            "total_revenue": to_float(total_revenue) or 0.0,
            "revenue_30d": to_float(revenue_30d) or 0.0,
            "pct_change_vs_prev_30d": to_float(pct_change),
            "mau_30d": int(mau_30d or 0),
            "pct_returning_30d": to_float(pct_returning) or 0.0
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

@router.get("/products-by-category")
async def products_by_category(
    categories: Optional[str] = Query(None, description="Comma-separated categories or 'All'"),
    min_price: Optional[float] = Query(None, ge=0.0),
    max_price: Optional[float] = Query(None, ge=0.0),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends( get_async_session)
):
    """
    Returns basic product stats for the given categories: product_id, name, price, units_sold, revenue.
    categories param: comma-separated string. If omitted or 'All', return all products.
    """
    try:
        params = {"limit": limit, "offset": offset}
        cat_clause = ""
        if categories and categories.lower() != "all":
            cats = [c.strip() for c in categories.split(",") if c.strip()]
            placeholders = []
            for i, c in enumerate(cats):
                key = f"cat{i}"
                params[key] = c
                placeholders.append(f":{key}")
            cat_clause = f"AND COALESCE(p.category,'Uncategorized') IN ({', '.join(placeholders)})"

        price_clause = ""
        if min_price is not None:
            price_clause += " AND p.price >= :min_price"
            params["min_price"] = float(min_price)
        if max_price is not None:
            price_clause += " AND p.price <= :max_price"
            params["max_price"] = float(max_price)

        sql = text(f"""
            SELECT
              p.product_id,
              p.name,
              p.price,
              COALESCE(SUM(oi.quantity), 0) AS units_sold,
              COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS revenue
            FROM ecom.products p
            LEFT JOIN ecom.order_items oi ON oi.product_id = p.product_id
            LEFT JOIN ecom.orders o ON o.order_id = oi.order_id AND o.status = 'completed'
            WHERE 1=1
              {cat_clause}
              {price_clause}
            GROUP BY p.product_id, p.name, p.price
            ORDER BY revenue DESC
            LIMIT :limit OFFSET :offset;
        """)
        result = await db.execute(sql, params)
        rows = result.fetchall()
        out = []
        for r in rows:
            out.append({
                "product_id": int(r[0]),
                "name": r[1],
                "price": float(r[2] or 0),
                "units_sold": int(r[3] or 0),
                "revenue": float(r[4] or 0)
            })
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# -----------------------
# /kpi/revenue-trend
# -----------------------
@router.get("/revenue-trend")
async def revenue_trend(
    period: str = Query("monthly", regex="^(daily|monthly)$"),
    months: int = Query(12, ge=1, le=60),
    start_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    end_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    db: AsyncSession = Depends( get_async_session)
):
    try:
                # start/end override (daily buckets)
                # start/end override (daily buckets)
                # start/end override (daily buckets)
        if start_date and end_date:
            # validate and convert to date objects
            try:
                sd = datetime.strptime(start_date, "%Y-%m-%d").date()
                ed = datetime.strptime(end_date, "%Y-%m-%d").date()
            except Exception:
                raise HTTPException(status_code=400, detail="start_date and end_date must be YYYY-MM-DD")

            # Build Python datetime objects (not strings) so asyncpg binds correctly
            from datetime import time as dt_time, timedelta as dt_timedelta
            start_dt = datetime.combine(sd, dt_time(0, 0, 0))
            end_dt = datetime.combine(ed, dt_time(23, 59, 59))
            # use exclusive upper bound to avoid messy interval arithmetic
            end_dt_plus_one = end_dt + dt_timedelta(seconds=1)

            sql = text("""
                SELECT date_trunc('day', o.order_date::timestamp)::date AS period,
                       SUM(oi.quantity * oi.unit_price) AS revenue
                FROM ecom.orders o
                JOIN ecom.order_items oi ON oi.order_id = o.order_id
                WHERE o.status = 'completed'
                  AND o.order_date::timestamp >= :start_date
                  AND o.order_date::timestamp < :end_date_plus_one
                GROUP BY 1
                ORDER BY 1;
            """)
            params = {"start_date": start_dt, "end_date_plus_one": end_dt_plus_one}
            result = await db.execute(sql, params)
            rows = result.fetchall()
            return [{"period": r[0], "revenue": float(r[1] or 0)} for r in rows]

        # monthly / daily fallback
        if period == "monthly":
            sql = text("""
                SELECT date_trunc('month', o.order_date::timestamp)::date AS period,
                       SUM(oi.quantity * oi.unit_price) AS revenue
                FROM ecom.orders o
                JOIN ecom.order_items oi ON oi.order_id = o.order_id
                WHERE o.status = 'completed'
                  AND o.order_date::timestamp >= date_trunc('month', now()) - INTERVAL :months_minus months
                GROUP BY 1
                ORDER BY 1;
            """)
            # SQLAlchemy text does not allow :months_minus in INTERVAL directly, so format
            sql = text(f"""
                SELECT date_trunc('month', o.order_date::timestamp)::date AS period,
                       SUM(oi.quantity * oi.unit_price) AS revenue
                FROM ecom.orders o
                JOIN ecom.order_items oi ON oi.order_id = o.order_id
                WHERE o.status = 'completed'
                  AND o.order_date::timestamp >= date_trunc('month', now()) - INTERVAL '{months - 1} months'
                GROUP BY 1
                ORDER BY 1;
            """)
            result = await db.execute(sql)
            rows = result.fetchall()
            return [{"period": r[0], "revenue": float(r[1] or 0)} for r in rows]
        else:
            days = months
            sql = text(f"""
                SELECT date_trunc('day', o.order_date::timestamp)::date AS period,
                       SUM(oi.quantity * oi.unit_price) AS revenue
                FROM ecom.orders o
                JOIN ecom.order_items oi ON oi.order_id = o.order_id
                WHERE o.status = 'completed'
                  AND o.order_date::timestamp >= now()::date - INTERVAL '{days - 1} days'
                GROUP BY 1
                ORDER BY 1;
            """)
            result = await db.execute(sql)
            rows = result.fetchall()
            return [{"period": r[0], "revenue": float(r[1] or 0)} for r in rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# -----------------------
# /kpi/top-products
# -----------------------
@router.get("/top-products")
async def top_products(
    limit: int = Query(20, ge=1, le=200),
    sort_by: str = Query("units", regex="^(units|revenue)$"),
    db: AsyncSession = Depends( get_async_session)
):
    try:
        order_clause = "total_units_sold" if sort_by == "units" else "total_revenue"
        sql = text(f"""
            SELECT
              p.product_id,
              p.name,
              SUM(oi.quantity) AS total_units_sold,
              SUM(oi.quantity * oi.unit_price) AS total_revenue
            FROM ecom.order_items oi
            JOIN ecom.orders o ON o.order_id = oi.order_id AND o.status = 'completed'
            JOIN ecom.products p ON p.product_id = oi.product_id
            GROUP BY p.product_id, p.name
            ORDER BY {order_clause} DESC
            LIMIT :limit;
        """)
        result = await db.execute(sql, {"limit": limit})
        rows = result.fetchall()
        out = []
        for r in rows:
            out.append({
                "product_id": int(r[0]),
                "name": r[1],
                "units_sold": int(r[2] or 0),
                "revenue": float(r[3] or 0)
            })
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# -----------------------
# /kpi/customer-insights
# -----------------------
@router.get("/customer-insights")
async def customer_insights(
    top_n: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends( get_async_session)
):
    try:
        top_sql = text("""
            SELECT
              u.user_id,
              COALESCE(u.email, '') AS email,
              SUM(oi.quantity * oi.unit_price) AS lifetime_revenue,
              COUNT(DISTINCT o.order_id) AS total_orders,
              MIN(o.order_date::timestamp) AS first_order,
              MAX(o.order_date::timestamp) AS last_order
            FROM ecom.users u
            LEFT JOIN ecom.orders o ON o.user_id = u.user_id AND o.status = 'completed'
            LEFT JOIN ecom.order_items oi ON oi.order_id = o.order_id
            GROUP BY u.user_id, u.email
            ORDER BY lifetime_revenue DESC
            LIMIT :top_n;
        """)
        r1 = await db.execute(top_sql, {"top_n": top_n})
        top_rows = r1.fetchall()
        top_customers = []
        for r in top_rows:
            top_customers.append({
                "user_id": int(r[0]),
                "email": r[1],
                "lifetime_revenue": float(r[2] or 0),
                "total_orders": int(r[3] or 0),
                "first_order": str(r[4]) if r[4] else None,
                "last_order": str(r[5]) if r[5] else None
            })

        nv_sql = text("""
            WITH recent AS (
              SELECT user_id, COUNT(DISTINCT order_id) AS cnt
              FROM ecom.orders
              WHERE status = 'completed'
                AND order_date::timestamp >= now()::date - INTERVAL '29 days'
              GROUP BY user_id
            )
            SELECT
              COUNT(*) FILTER (WHERE cnt = 1) AS new_customers,
              COUNT(*) FILTER (WHERE cnt > 1) AS repeat_customers,
              COUNT(*) AS total_customers,
              ROUND(100.0 * SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END)::numeric / NULLIF(COUNT(*),0),2) AS pct_repeat
            FROM recent;
        """)
        r2 = await db.execute(nv_sql)
        nv = r2.fetchone()
        new_vs_repeat = {
            "new_customers": int(nv[0] or 0),
            "repeat_customers": int(nv[1] or 0),
            "total_customers": int(nv[2] or 0),
            "pct_repeat": float(nv[3] or 0)
        }

        return {
            "top_customers": top_customers,
            "new_vs_repeat": new_vs_repeat
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# -----------------------
# /kpi/reviews  (fixed)
# -----------------------
@router.get("/reviews")
async def recent_reviews(
    limit: int = Query(50, ge=1, le=500),
    min_rating: int = Query(0, ge=0, le=5),
    db: AsyncSession = Depends( get_async_session)
):
    """
    Returns recent reviews filtered by minimum rating (default 0 = all).
    Uses a sentinel value (0) to mean 'no filter' so the DB parameter has a concrete type.
    """
    try:
        sql = text("""
            SELECT review_id,
                   user_id,
                   product_id,
                   rating,
                   review_text,
                   review_date
            FROM ecom.product_reviews
            WHERE (:min_rating = 0 OR rating >= :min_rating)
            ORDER BY review_date DESC
            LIMIT :limit;
        """)
        params = {"limit": int(limit), "min_rating": int(min_rating)}
        result = await db.execute(sql, params)
        rows = result.fetchall()

        out = []
        for r in rows:
            review_id = int(r[0])
            user_id = int(r[1]) if r[1] is not None else None
            product_id = int(r[2]) if r[2] is not None else None
            rating = int(r[3]) if r[3] is not None else None
            review_text = r[4] or ""
            review_date_text = r[5]  # keep as text; may be None or malformed

            # basic keyword heuristic for sentiment
            text_lower = review_text.lower()
            if any(k in text_lower for k in ["great", "excellent", "love", "awesome", "amazing"]):
                sentiment = "positive"
            elif any(k in text_lower for k in ["bad", "terrible", "hate", "awful", "disappoint"]):
                sentiment = "negative"
            else:
                sentiment = "neutral"

            # try safe parse of review_date to ISO string; fall back to raw text
            parsed_date = None
            if review_date_text:
                try:
                    parsed_date = datetime.fromisoformat(review_date_text).isoformat()
                except Exception:
                    parsed_date = review_date_text

            out.append({
                "review_id": review_id,
                "user_id": user_id,
                "product_id": product_id,
                "rating": rating,
                "review_text": review_text,
                "review_date": parsed_date,
                "sentiment": sentiment
            })

        return out

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")
# -----------------------
# /kpi/categories
# -----------------------
@router.get("/categories")
async def list_categories(db: AsyncSession = Depends( get_async_session)):
    """
    Return distinct product categories (string) from ecom.products
    """
    try:
        sql = text("""
            SELECT DISTINCT COALESCE(category, 'Uncategorized') AS category
            FROM ecom.products
            ORDER BY category;
        """)
        result = await db.execute(sql)
        rows = result.fetchall()
        return [r[0] for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# -----------------------
# /kpi/revenue-by-category
# -----------------------
@router.get("/revenue-by-category")
async def revenue_by_category(
    months: int = Query(6, ge=1, le=60),
    db: AsyncSession = Depends( get_async_session)
):
    """
    Returns aggregated revenue per category for the past `months` months.
    """
    try:
        sql = text(f"""
            SELECT COALESCE(p.category, 'Uncategorized') AS category,
                   SUM(oi.quantity * oi.unit_price) AS revenue,
                   SUM(oi.quantity) AS units_sold
            FROM ecom.order_items oi
            JOIN ecom.orders o ON o.order_id = oi.order_id AND o.status = 'completed'
            JOIN ecom.products p ON p.product_id = oi.product_id
            WHERE o.order_date::timestamp >= date_trunc('month', now()) - INTERVAL '{months - 1} months'
            GROUP BY 1
            ORDER BY revenue DESC;
        """)
        result = await db.execute(sql)
        rows = result.fetchall()
        return [{"category": r[0], "revenue": float(r[1] or 0), "units_sold": int(r[2] or 0)} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


# -----------------------
# Extend /kpi/top-products: allow category filter
# (If you already have top_products, replace its signature/body with this one)
# -----------------------
@router.get("/top-products")
async def top_products(
    limit: int = Query(20, ge=1, le=200),
    sort_by: str = Query("units", regex="^(units|revenue)$"),
    category: Optional[str] = Query(None, description="Filter by category"),
    db: AsyncSession = Depends( get_async_session)
):
    """
    Top products optionally filtered by category.
    """
    try:
        order_clause = "total_units_sold" if sort_by == "units" else "total_revenue"

        if category:
            sql = text(f"""
                SELECT
                  p.product_id,
                  p.name,
                  SUM(oi.quantity) AS total_units_sold,
                  SUM(oi.quantity * oi.unit_price) AS total_revenue
                FROM ecom.order_items oi
                JOIN ecom.orders o ON o.order_id = oi.order_id AND o.status = 'completed'
                JOIN ecom.products p ON p.product_id = oi.product_id
                WHERE p.category = :category
                GROUP BY p.product_id, p.name
                ORDER BY {order_clause} DESC
                LIMIT :limit;
            """)
            params = {"limit": limit, "category": category}
        else:
            sql = text(f"""
                SELECT
                  p.product_id,
                  p.name,
                  SUM(oi.quantity) AS total_units_sold,
                  SUM(oi.quantity * oi.unit_price) AS total_revenue
                FROM ecom.order_items oi
                JOIN ecom.orders o ON o.order_id = oi.order_id AND o.status = 'completed'
                JOIN ecom.products p ON p.product_id = oi.product_id
                GROUP BY p.product_id, p.name
                ORDER BY {order_clause} DESC
                LIMIT :limit;
            """)
            params = {"limit": limit}

        result = await db.execute(sql, params)
        rows = result.fetchall()
        out = []
        for r in rows:
            out.append({
                "product_id": int(r[0]),
                "name": r[1],
                "units_sold": int(r[2] or 0),
                "revenue": float(r[3] or 0)
            })
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")

# -----------------------
# /kpi/products-list
# -----------------------
@router.get("/products-list")
async def products_list(limit: int = Query(1000, ge=1, le=5000), db: AsyncSession = Depends( get_async_session)):
    """
    Return simple product listing (id, name) used by UI dropdowns.
    """
    try:
        sql = text("""
            SELECT product_id, name
            FROM ecom.products
            ORDER BY name
            LIMIT :limit;
        """)
        result = await db.execute(sql, {"limit": int(limit)})
        rows = result.fetchall()
        return [{"product_id": int(r[0]), "name": r[1]} for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query error: {str(e)}")


@router.get("/recommendations")
async def recommendations(
    product_id: int = Query(..., description="Product ID to get recommendations for"),
    limit: int = Query(10, ge=1, le=50),
    method: str = Query("co-purchase", regex="^(co-purchase|category)$"),
    db: AsyncSession = Depends( get_async_session)
):
    """
    Returns recommended products for a given product_id.
    method:
      - 'co-purchase' (default): items most frequently bought together with product_id
      - 'category': fallback: top products in the same category by revenue
    """
    try:
        if method == "co-purchase":
            # 1) compute co-purchase counts (other items in same orders)
            co_sql = text("""
                SELECT
                  oi2.product_id as other_product_id,
                  p.name,
                  COUNT(DISTINCT oi1.order_id) AS co_count,
                  SUM(oi2.quantity * oi2.unit_price) AS co_revenue
                FROM ecom.order_items oi1
                JOIN ecom.order_items oi2
                  ON oi1.order_id = oi2.order_id
                  AND oi1.product_id != oi2.product_id
                JOIN ecom.products p ON p.product_id = oi2.product_id
                WHERE oi1.product_id = :product_id
                GROUP BY oi2.product_id, p.name
                ORDER BY co_count DESC, co_revenue DESC
                LIMIT :limit;
            """)
            r = await db.execute(co_sql, {"product_id": int(product_id), "limit": int(limit)})
            rows = r.fetchall()

            # get total orders that contain product_id to compute support
            total_orders_sql = text("""
                SELECT COUNT(DISTINCT order_id) FROM ecom.order_items WHERE product_id = :product_id;
            """)
            total_res = await db.execute(total_orders_sql, {"product_id": int(product_id)})
            total_orders = total_res.scalar() or 0

            out = []
            for row in rows:
                other_id = int(row[0])
                name = row[1]
                co_count = int(row[2] or 0)
                co_revenue = float(row[3] or 0.0)
                support = (co_count / total_orders) if total_orders > 0 else 0.0
                out.append({
                    "product_id": other_id,
                    "name": name,
                    "co_count": co_count,
                    "co_revenue": co_revenue,
                    "support": round(support, 4)
                })
            return out

        else:  # category fallback
            # find the category of the product
            cat_sql = text("SELECT category FROM ecom.products WHERE product_id = :product_id LIMIT 1;")
            cres = await db.execute(cat_sql, {"product_id": int(product_id)})
            crow = cres.fetchone()
            if not crow or not crow[0]:
                raise HTTPException(status_code=404, detail="Product or category not found for fallback.")
            category = crow[0]

            # find top products in same category by revenue
            sql = text("""
                SELECT p.product_id, p.name, COALESCE(SUM(oi.quantity * oi.unit_price), 0) AS revenue
                FROM ecom.products p
                LEFT JOIN ecom.order_items oi ON oi.product_id = p.product_id
                LEFT JOIN ecom.orders o ON o.order_id = oi.order_id AND o.status = 'completed'
                WHERE COALESCE(p.category,'Uncategorized') = :category
                  AND p.product_id != :product_id
                GROUP BY p.product_id, p.name
                ORDER BY revenue DESC
                LIMIT :limit;
            """)
            rr = await db.execute(sql, {"category": category, "product_id": int(product_id), "limit": int(limit)})
            rows = rr.fetchall()
            return [{"product_id": int(r[0]), "name": r[1], "revenue": float(r[2] or 0.0)} for r in rows]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recommendation error: {str(e)}")
