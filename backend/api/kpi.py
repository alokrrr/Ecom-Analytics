# backend/api/kpi.py

from typing import Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db import get_async_session

router = APIRouter()


# -----------------------
# /kpi/overview
# -----------------------
@router.get("/overview")
async def kpi_overview(db: AsyncSession = Depends(get_async_session)):
    try:
        sql = text("""
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
        mau_30d AS (
          SELECT COUNT(DISTINCT user_id) AS mau_30d
          FROM ecom.orders
          WHERE status = 'completed'
            AND order_date::timestamp >= now()::date - INTERVAL '29 days'
        )
        SELECT
          tr.total_revenue,
          r30.revenue_30d,
          m.mau_30d
        FROM total_rev tr
        CROSS JOIN rev_30d r30
        CROSS JOIN mau_30d m;
        """)

        res = await db.execute(sql)
        row = res.fetchone()

        return {
            "total_revenue": float(row[0] or 0),
            "revenue_30d": float(row[1] or 0),
            "mau_30d": int(row[2] or 0),
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# /kpi/categories
# -----------------------
@router.get("/categories")
async def list_categories(db: AsyncSession = Depends(get_async_session)):
    try:
        sql = text("""
            SELECT DISTINCT COALESCE(category, 'Uncategorized')
            FROM ecom.products
            ORDER BY 1;
        """)
        res = await db.execute(sql)
        return [r[0] for r in res.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# /kpi/revenue-trend
# -----------------------
@router.get("/revenue-trend")
async def revenue_trend(
    months: int = Query(12, ge=1, le=60),
    db: AsyncSession = Depends(get_async_session)
):
    try:
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
        res = await db.execute(sql)
        return [{"period": r[0], "revenue": float(r[1] or 0)} for r in res.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# /kpi/top-products
# -----------------------
@router.get("/top-products")
async def top_products(
    limit: int = Query(20, ge=1, le=200),
    db: AsyncSession = Depends(get_async_session)
):
    try:
        sql = text("""
            SELECT
              p.product_id,
              p.name,
              SUM(oi.quantity) AS units_sold,
              SUM(oi.quantity * oi.unit_price) AS revenue
            FROM ecom.order_items oi
            JOIN ecom.orders o ON o.order_id = oi.order_id AND o.status = 'completed'
            JOIN ecom.products p ON p.product_id = oi.product_id
            GROUP BY p.product_id, p.name
            ORDER BY revenue DESC
            LIMIT :limit;
        """)
        res = await db.execute(sql, {"limit": limit})
        return [
            {
                "product_id": int(r[0]),
                "name": r[1],
                "units_sold": int(r[2] or 0),
                "revenue": float(r[3] or 0),
            }
            for r in res.fetchall()
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# /kpi/products-list
# -----------------------
@router.get("/products-list")
async def products_list(
    limit: int = Query(1000, ge=1, le=5000),
    db: AsyncSession = Depends(get_async_session)
):
    try:
        sql = text("""
            SELECT product_id, name
            FROM ecom.products
            ORDER BY name
            LIMIT :limit;
        """)
        res = await db.execute(sql, {"limit": limit})
        return [{"product_id": int(r[0]), "name": r[1]} for r in res.fetchall()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# -----------------------
# /kpi/recommendations
# -----------------------
@router.get("/recommendations")
async def recommendations(
    product_id: int,
    limit: int = Query(10, ge=1, le=50),
    db: AsyncSession = Depends(get_async_session)
):
    try:
        sql = text("""
            SELECT
              oi2.product_id,
              p.name,
              COUNT(*) AS co_count
            FROM ecom.order_items oi1
            JOIN ecom.order_items oi2 ON oi1.order_id = oi2.order_id
            JOIN ecom.products p ON p.product_id = oi2.product_id
            WHERE oi1.product_id = :product_id
              AND oi2.product_id != :product_id
            GROUP BY oi2.product_id, p.name
            ORDER BY co_count DESC
            LIMIT :limit;
        """)
        res = await db.execute(
            sql, {"product_id": product_id, "limit": limit}
        )
        return [
            {"product_id": int(r[0]), "name": r[1], "co_count": int(r[2])}
            for r in res.fetchall()
        ]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
