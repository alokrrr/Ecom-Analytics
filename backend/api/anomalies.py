# backend/api/anomalies.py
import math
from datetime import date, datetime, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from backend.db import get_db

router = APIRouter()

# --- Pydantic models ---
class SeriesPoint(BaseModel):
    day: date
    revenue: float

class AnomalyPoint(BaseModel):
    day: date
    revenue: float
    score: float  # z-score or outlier-score
    reason: str

class AnomalyResponse(BaseModel):
    series: List[SeriesPoint]
    anomalies: List[AnomalyPoint]
    method: str
    window: int
    threshold: float

# --- Helpers ---
# --- replace existing _get_daily_revenue with this robust version ---
from datetime import datetime as _dt, time as _dt_time, timedelta as _dt_timedelta
from sqlalchemy import text as _text

async def _get_daily_revenue(db: AsyncSession, start_date: date, end_date: date):
    """
    Return daily revenue series between start_date and end_date (inclusive).
    Uses orders.order_date and order_items to compute revenue if orders.total_amount not present.
    Accepts start_date/end_date as date objects or 'YYYY-MM-DD' strings.
    Returns list of tuples (date, revenue_float).
    """
    # Normalize input to date objects
    if isinstance(start_date, str):
        sd = _dt.strptime(start_date, "%Y-%m-%d").date()
    else:
        sd = start_date
    if isinstance(end_date, str):
        ed = _dt.strptime(end_date, "%Y-%m-%d").date()
    else:
        ed = end_date

    # Build datetime bounds (inclusive start, exclusive end)
    start_dt = _dt.combine(sd, _dt_time(0, 0, 0))
    end_dt = _dt.combine(ed, _dt_time(23, 59, 59))
    end_dt_plus_one = end_dt + _dt_timedelta(seconds=1)

    # 1) try to use orders.total_amount if present, aggregated by order_date
    sql_try_total = _text("""
    SELECT date_trunc('day', o.order_date::timestamp)::date AS day,
           SUM(o.total_amount)::numeric AS revenue
    FROM ecom.orders o
    WHERE o.order_date::timestamp >= :start_dt
      AND o.order_date::timestamp < :end_dt_plus_one
      AND (o.status = 'completed' OR :include_all = true)
    GROUP BY day
    ORDER BY day;
    """)
    params = {"start_dt": start_dt, "end_dt_plus_one": end_dt_plus_one, "include_all": False}
    try:
        res = await db.execute(sql_try_total, params)
        rows = res.fetchall()
    except Exception:
        rows = []

    if rows:
        return [(r[0], float(r[1] or 0.0)) for r in rows]

    # 2) fallback: compute from order_items (use order_id and order_date names consistent with kpi.py)
    sql_from_items = _text("""
    SELECT date_trunc('day', o.order_date::timestamp)::date AS day,
           COALESCE(SUM(oi.quantity * COALESCE(oi.unit_price,0)), 0)::numeric AS revenue
    FROM ecom.orders o
    LEFT JOIN ecom.order_items oi ON oi.order_id = o.order_id
    WHERE o.order_date::timestamp >= :start_dt
      AND o.order_date::timestamp < :end_dt_plus_one
      AND (o.status = 'completed' OR :include_all = true)
    GROUP BY day
    ORDER BY day;
    """)
    params2 = {"start_dt": start_dt, "end_dt_plus_one": end_dt_plus_one, "include_all": False}
    res2 = await db.execute(sql_from_items, params2)
    rows2 = res2.fetchall()
    return [(r[0], float(r[1] or 0.0)) for r in rows2]

    


def _fill_missing_days(series: List[tuple], start_date: date, end_date: date):
    """Ensure contiguous daily series between start_date and end_date (inclusive)."""
    d = start_date
    idx = {s[0]: s[1] for s in series}
    out = []
    while d <= end_date:
        out.append((d, idx.get(d, 0.0)))
        d += timedelta(days=1)
    return out

def _detect_zscore(series_vals: List[float], window: int, threshold: float):
    """
    Rolling z-score detection. For each point i:
      - compute mean/std of previous `window` days (not including current)
      - z = (value - mean)/std
      - mark as anomaly if abs(z) >= threshold
    Returns list of (index, score) for anomalies where score = z
    """
    n = len(series_vals)
    anomalies = []
    for i in range(n):
        start = max(0, i - window)
        end = i  # previous values only
        window_vals = series_vals[start:end]
        if len(window_vals) < 2:
            continue
        mean = sum(window_vals)/len(window_vals)
        var = sum((x-mean)**2 for x in window_vals) / (len(window_vals)-0)  # population variance ok
        std = math.sqrt(var) if var > 0 else 0.0
        if std == 0:
            continue
        z = (series_vals[i] - mean) / std
        if abs(z) >= threshold:
            anomalies.append((i, z))
    return anomalies

def _detect_iqr(series_vals: List[float], window: int, threshold: float):
    """
    Rolling IQR detection. For each point uses previous `window` days IQR to mark outliers.
    threshold is multiplier of IQR (e.g., 1.5).
    Returns list of (index, score) where score = (value - Q3) / IQR or (Q1 - value)/IQR depending which side
    """
    import statistics
    n = len(series_vals)
    anomalies = []
    for i in range(n):
        start = max(0, i - window)
        end = i
        window_vals = series_vals[start:end]
        if len(window_vals) < 4:
            continue
        q1 = statistics.quantiles(window_vals, n=4)[0]  # first quartile
        q3 = statistics.quantiles(window_vals, n=4)[2]  # third quartile
        iqr = q3 - q1
        if iqr == 0:
            continue
        val = series_vals[i]
        # outlier if val > q3 + threshold * IQR or val < q1 - threshold * IQR
        if val > q3 + threshold * iqr:
            score = (val - q3) / iqr
            anomalies.append((i, score))
        elif val < q1 - threshold * iqr:
            score = (q1 - val) / iqr
            anomalies.append((i, score))
    return anomalies

# --- Endpoints ---

@router.get("/anomalies/series", response_model=List[SeriesPoint])
async def get_revenue_series(
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    # Defaults: last 90 days
    today = date.today()
    if end_date is None:
        end_date = today
    if start_date is None:
        start_date = today - timedelta(days=89)

    series = await _get_daily_revenue(db, start_date, end_date)
    series_filled = _fill_missing_days(series, start_date, end_date)
    return [{"day": d, "revenue": r} for d, r in series_filled]


@router.get("/anomalies/detect", response_model=AnomalyResponse)
async def detect_anomalies(
    method: str = Query("zscore", regex="^(zscore|iqr)$"),
    window: int = Query(7, ge=2, le=90),
    threshold: float = Query(3.0, gt=0),
    start_date: Optional[date] = Query(None),
    end_date: Optional[date] = Query(None),
    db: AsyncSession = Depends(get_db),
):
    """
    Detect anomalies on daily revenue.
    - method: 'zscore' or 'iqr'
    - window: lookback window in days for rolling stats (previous days only)
    - threshold:
        * for zscore: absolute z threshold (e.g., 3.0)
        * for iqr: multiplier of IQR (e.g., 1.5)
    """
    today = date.today()
    if end_date is None:
        end_date = today
    if start_date is None:
        start_date = today - timedelta(days=89)

    if start_date > end_date:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    # Fetch series
    raw_series = await _get_daily_revenue(db, start_date, end_date)
    series = _fill_missing_days(raw_series, start_date, end_date)
    days = [d for d, _ in series]
    values = [v for _, v in series]

    if method == "zscore":
        raw_anoms = _detect_zscore(values, window=window, threshold=threshold)
        anomalies = [{"index": idx, "score": score} for idx, score in raw_anoms]
    else:
        raw_anoms = _detect_iqr(values, window=window, threshold=threshold)
        anomalies = [{"index": idx, "score": score} for idx, score in raw_anoms]

    # Format anomalies
    anom_points = []
    for a in anomalies:
        idx = a["index"]
        score = a["score"]
        anom_points.append({
            "day": days[idx],
            "revenue": values[idx],
            "score": float(score),
            "reason": f"{method} (window={window}, threshold={threshold})"
        })

    # Build response
    series_model = [{"day": d, "revenue": v} for d, v in series]
    anomalies_model = [
        {"day": p["day"], "revenue": p["revenue"], "score": p["score"], "reason": p["reason"]}
        for p in anom_points
    ]
    return {
        "series": series_model,
        "anomalies": anomalies_model,
        "method": method,
        "window": window,
        "threshold": threshold
    }
