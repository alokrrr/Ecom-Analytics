# backend/main.py

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("backend.main")

app = FastAPI(
    title="E-Commerce Analytics API",
    version="1.0.0",
)

# -----------------------
# CORS (safe for frontend usage)
# -----------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -----------------------
# Routers
# -----------------------
from backend.api.kpi import router as kpi_router
from backend.api.anomalies import router as anomalies_router

app.include_router(kpi_router, prefix="/kpi")
log.info("KPI router loaded")

app.include_router(anomalies_router, prefix="/kpi")
log.info("Anomalies router loaded")

# -----------------------
# Root health
# -----------------------
@app.get("/")
async def root():
    return {"status": "running"}

# -----------------------
# Startup log (helps on Render)
# -----------------------
@app.on_event("startup")
async def startup_event():
    log.info("Application startup complete")
    log.info("Registered routes:")
    for r in app.routes:
        log.info("  %s %s", r.path, getattr(r, "methods", None))
