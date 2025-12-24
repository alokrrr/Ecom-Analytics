# scripts/debug_anomalies.py
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

# Example params (you can change dates if you like)
params = {
    "method": "zscore",
    "window": 7,
    "threshold": 3.0,
    "start_date": "2025-09-01",
    "end_date": "2025-11-29"
}

print("Calling /kpi/anomalies/detect with params:", params)
resp = client.get("/kpi/anomalies/detect", params=params)
print("Status code:", resp.status_code)
try:
    print("Response JSON:", resp.json())
except Exception:
    print("Response text:", resp.text)

if resp.status_code >= 500:
    print("\nRe-calling with server exceptions enabled to capture traceback...")
    client.raise_server_exceptions = True
    try:
        client.get("/kpi/anomalies/detect", params=params)
    except Exception:
        print("\n--- SERVER TRACEBACK START ---")
        traceback.print_exc()
        print("--- SERVER TRACEBACK END ---")
