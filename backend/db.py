# backend/db.py
import os
import ssl
import logging
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

# -------------------------------------------------
# Logging
# -------------------------------------------------
log = logging.getLogger("backend.db")
log.setLevel(logging.INFO)

# -------------------------------------------------
# Load .env (local only; Render uses env vars)
# -------------------------------------------------
proj_root = Path(__file__).resolve().parents[1]
load_dotenv(proj_root / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# -------------------------------------------------
# Clean unsupported query params (asyncpg-safe)
# -------------------------------------------------
parts = urlparse(DATABASE_URL)
qs = dict(parse_qsl(parts.query, keep_blank_values=True))
for bad in ("sslmode", "sslcert", "sslkey", "sslrootcert"):
    qs.pop(bad, None)

clean_url = urlunparse((
    parts.scheme,
    parts.netloc,   # ✅ DO NOT MODIFY HOSTNAME
    parts.path,
    parts.params,
    urlencode(qs),
    parts.fragment,
))

# -------------------------------------------------
# Ensure asyncpg driver
# -------------------------------------------------
if clean_url.startswith("postgresql://"):
    clean_url = clean_url.replace(
        "postgresql://", "postgresql+asyncpg://", 1
    )
elif clean_url.startswith("postgres://"):
    clean_url = clean_url.replace(
        "postgres://", "postgresql+asyncpg://", 1
    )

log.info("Connecting to database (host preserved, SSL enabled)")

# -------------------------------------------------
# SSL (required by Supabase)
# -------------------------------------------------
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

# -------------------------------------------------
# Create engine
# -------------------------------------------------
engine = create_async_engine(
    clean_url,
    echo=False,
    future=True,
    poolclass=NullPool,
    connect_args={"ssl": ssl_ctx},
)

# -------------------------------------------------
# Session factory
# -------------------------------------------------
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# -------------------------------------------------
# ✅ SINGLE OFFICIAL DEPENDENCY (use everywhere)
# -------------------------------------------------
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
