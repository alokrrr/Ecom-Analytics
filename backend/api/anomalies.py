# backend/api/anomalies.py

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from backend.db import get_async_session

router = APIRouter()

# -------------------------------------------------
# Health check (used to verify router loading)
# -------------------------------------------------
@router.get("/anomalies/health")
async def anomalies_health():
    return {"status": "ok", "service": "anomalies"}

# -------------------------------------------------
# Revenue anomaly detection (simple z-score logic)
# -------------------------------------------------
@router.get("/anomalies/revenue")
async def revenue_anomalies(
    threshold: float = 2.0,
    db: AsyncSession = Depends(get_async_session),
):
    """
    Detect revenue anomalies using simple z-score method.
    Returns days where revenue deviates significantly from mean.
    """
    try:
        sql = text("""
            SELECT
                date_trunc('day', o.order_date::timestamp)::date AS day,
                SUM(oi.quantity * oi.unit_price) AS revenue
            FROM ecom.orders o
            JOIN ecom.order_items oi ON oi.order_id = o.order_id
            WHERE o.status = 'completed'
            GROUP BY 1
            ORDER BY 1;
        """)

        res = await db.execute(sql)
        rows = res.fetchall()

        if not rows:
            return {"anomalies": [], "message": "No data available"}

        # Compute mean and std in Python
        revenues = [float(r[1] or 0) for r in rows]
        mean = sum(revenues) / len(revenues)

        variance = sum((x - mean) ** 2 for x in revenues) / len(revenues)
        std = variance ** 0.5

        anomalies = []
        for (day, revenue), value in zip(rows, revenues):
            if std > 0:
                z = (value - mean) / std
                if abs(z) >= threshold:
                    anomalies.append({
                        "date": str(day),
                        "revenue": value,
                        "z_score": round(z, 2)
                    })

        return {
            "mean_revenue": round(mean, 2),
            "std_dev": round(std, 2),
            "threshold": threshold,
            "anomalies": anomalies
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
