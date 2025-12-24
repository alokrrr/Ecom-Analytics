# scripts/debug_revenue_trend.py
import sys, traceback
sys.path.append(".")

from fastapi.testclient import TestClient

try:
    from backend.main import app
except Exception:
    print("ERROR importing backend.main:")
    traceback.print_exc()
    raise SystemExit(1)

client = TestClient(app)

# adjust these dates if you want; this matches the last 7 days by default
from datetime import date, timedelta
sd = (date.today() - timedelta(days=7)).isoformat()
ed = date.today().isoformat()

params = {"period": "daily", "start_date": sd, "end_date": ed}
print("Calling /kpi/revenue-trend with params:", params)

# First, a plain request to show status + body
resp = client.get("/kpi/revenue-trend", params=params)
print("Status code:", resp.status_code)
try:
    print("Response JSON:", resp.json())
except Exception:
    print("Response text:", resp.text)

# If server returned 500, re-call with server-exceptions turned on to show traceback
if resp.status_code >= 500:
    print("\nAttempting to re-call with server exceptions enabled to capture traceback...")
    client.raise_server_exceptions = True
    try:
        resp2 = client.get("/kpi/revenue-trend", params=params)
        print("Second call status:", resp2.status_code)
        try:
            print("Second JSON:", resp2.json())
        except Exception:
            print("Second text:", resp2.text)
    except Exception:
        print("\n--- SERVER TRACEBACK START ---")
        traceback.print_exc()
        print("--- SERVER TRACEBACK END ---")
