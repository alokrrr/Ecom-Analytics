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

log = logging.getLogger("backend.db")
log.setLevel(logging.INFO)

# Load env (local only; Render uses dashboard vars)
proj_root = Path(__file__).resolve().parents[1]
load_dotenv(proj_root / ".env")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# Clean URL for asyncpg
parts = urlparse(DATABASE_URL)
qs = dict(parse_qsl(parts.query, keep_blank_values=True))
for k in ("sslmode", "sslcert", "sslkey", "sslrootcert"):
    qs.pop(k, None)

clean_url = urlunparse((
    parts.scheme,
    parts.netloc,   # ✅ NEVER touch hostname
    parts.path,
    parts.params,
    urlencode(qs),
    parts.fragment,
))

# Force asyncpg
if clean_url.startswith("postgresql://"):
    clean_url = clean_url.replace("postgresql://", "postgresql+asyncpg://", 1)
elif clean_url.startswith("postgres://"):
    clean_url = clean_url.replace("postgres://", "postgresql+asyncpg://", 1)

log.info("Connecting to Supabase via Session Pooler")

# SSL (Supabase requires it)
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

engine = create_async_engine(
    clean_url,
    echo=False,
    future=True,
    poolclass=NullPool,
    connect_args={"ssl": ssl_ctx},  # ✅ NEVER None
)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# ✅ ONLY dependency to use everywhere
async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        yield session
