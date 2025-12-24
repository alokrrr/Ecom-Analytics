# scripts/debug_endpoints.py
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

endpoints = [
    ("/kpi/categories", {"name": "categories"}),
    ("/kpi/overview", {"name": "overview"})
]

for path, meta in endpoints:
    print("="*80)
    print(f"CALLING {path}")
    try:
        resp = client.get(path)
        print("Status code:", resp.status_code)
        try:
            print("Response JSON:", resp.json())
        except Exception:
            print("Response text:", resp.text)
    except Exception:
        # re-call with server exceptions enabled to capture traceback
        print("Exception during call — re-calling with server exceptions to show traceback...")
        client.raise_server_exceptions = True
        try:
            client.get(path)
        except Exception:
            print("\n--- SERVER TRACEBACK START ---")
            traceback.print_exc()
            print("--- SERVER TRACEBACK END ---")
    print()
