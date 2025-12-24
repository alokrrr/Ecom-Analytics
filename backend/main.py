# backend/main.py  -- verbose, fail-fast version for debugging router includes
import logging
import sys
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("backend.main")

# Ensure project root on path (helps if uvicorn launched from other cwd)
sys.path.append(".")

app = FastAPI(title="E-Commerce Analytics API (debug)", version="1.0.0")

# CORS (keep permissive for local testing)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Explicitly import and include routers WITHOUT try/except so import errors aren't swallowed
# If any import fails, Python will raise and you'll see the traceback in the server terminal.
from backend.api.kpi import router as kpi_router
from backend.api.anomalies import router as anomalies_router

# include them
app.include_router(kpi_router, prefix="/kpi")
log.info("Included kpi_router")
app.include_router(anomalies_router, prefix="/kpi")
log.info("Included anomalies_router")

# Print all registered routes for debugging
log.info("Registered routes (BEGIN):")
for r in app.routes:
    log.info("  %s   %s", r.path, getattr(r, "methods", None))
log.info("Registered routes (END)")

@app.get("/")
def root():
    return {"status": "running", "debug": True}

## add near the bottom of backend/main.py, after routers included
# remove or comment out this function in backend/main.py
@app.get("/kpi/anomalies/health")
def anomalies_health_direct():
    return {"ok": True, "source": "main_direct"}
