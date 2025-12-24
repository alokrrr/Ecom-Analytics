# backend/db.py
import os
import socket
import ssl
import logging
from pathlib import Path
from dotenv import load_dotenv
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

log = logging.getLogger("backend.db")
log.setLevel(logging.INFO)

# Load .env from project root
proj_root = Path(__file__).resolve().parents[1]
env_path = proj_root / ".env"
load_dotenv(env_path)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
if not DATABASE_URL:
    raise RuntimeError(f"DATABASE_URL not set. Expected .env at: {env_path}")

def _mask_dburl(url: str) -> str:
    """Return a masked representation of a DB URL for logging (no secrets)."""
    try:
        p = urlparse(url)
        userinfo = "<user:pass>" if p.username else ""
        hostport = f"{p.hostname}:{p.port}" if p.port else (p.hostname or "")
        query = f"?{p.query}" if p.query else ""
        return f"{p.scheme}://{userinfo}@{hostport}{p.path or ''}{query}"
    except Exception:
        return "<unable to mask>"

log.info("Using DATABASE_URL (masked): %s", _mask_dburl(DATABASE_URL))

# Parse the URL and remove unsupported query params (e.g. sslmode) for asyncpg
parts = urlparse(DATABASE_URL)
qs = dict(parse_qsl(parts.query or "", keep_blank_values=True))
for bad in ["sslmode", "sslcert", "sslkey", "sslrootcert"]:
    if bad in qs:
        qs.pop(bad, None)
new_query = urlencode(qs, doseq=True)

# Resolve host to prefer IPv4 (dev fallback). If IPv4 A record exists, inject it into the netloc.
# If only IPv6 exists, use it (wrapped in brackets).
def _resolve_host_prefer_ipv4(hostname: str):
    """Return tuple (resolved_ip, is_ipv6) or (None, False) if resolution failed."""
    ipv4 = None
    ipv6 = None
    try:
        # getaddrinfo may return multiple addresses; inspect families
        for ai in socket.getaddrinfo(hostname, None):
            fam = ai[0]
            sockaddr = ai[4]
            if fam == socket.AF_INET and not ipv4:
                ipv4 = sockaddr[0]
            elif fam == socket.AF_INET6 and not ipv6:
                ipv6 = sockaddr[0]
        if ipv4:
            return ipv4, False
        if ipv6:
            return ipv6, True
    except Exception as e:
        log.info("Host resolution attempt for %s failed: %s", hostname, e)
    return None, False

parsed = parts
host = parsed.hostname
port = parsed.port
username = parsed.username
password = parsed.password

resolved_ip, resolved_is_ipv6 = _resolve_host_prefer_ipv4(host)

# Build netloc: preserve username:password@ and port; inject resolved IP if available
if resolved_ip:
    if username:
        userinfo = username
        if password:
            userinfo += f":{password}"
        userinfo += "@"
    else:
        userinfo = ""
    if resolved_is_ipv6:
        host_part = f"[{resolved_ip}]"
    else:
        host_part = resolved_ip
    if port:
        netloc = f"{userinfo}{host_part}:{port}"
    else:
        netloc = f"{userinfo}{host_part}"
    log.info("Resolved DB host %s -> %s (ipv6=%s). Injecting into DB URL for this run (dev fallback).",
             host, resolved_ip, resolved_is_ipv6)
else:
    # No resolution to an IP (or failure) — use original netloc (may be hostname or host:port)
    netloc = parsed.netloc
    log.info("No IP resolved for DB host %s; using original netloc (%s).", host, netloc)

# Rebuild cleaned URL with possibly replaced netloc
clean_parts = (parsed.scheme, netloc, parsed.path, parsed.params, new_query, parsed.fragment)
CLEAN_DB_URL = urlunparse(clean_parts)

# Convert to SQLAlchemy asyncpg-compatible URL
if CLEAN_DB_URL.startswith("postgres://"):
    DATABASE_URL_ASYNC = CLEAN_DB_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif CLEAN_DB_URL.startswith("postgresql://"):
    DATABASE_URL_ASYNC = CLEAN_DB_URL.replace("postgresql://", "postgresql+asyncpg://", 1)
else:
    DATABASE_URL_ASYNC = CLEAN_DB_URL

# Prepare SSL context for asyncpg connect args if needed.
# Detect whether an SSL intent exists by checking original query params (qs may have had sslmode before removal).
use_ssl = False
# If original DATABASE_URL contained sslmode=require or similar, treat as SSL required.
orig_qs = dict(parse_qsl(parts.query or "", keep_blank_values=True))
if "sslmode" in orig_qs and orig_qs.get("sslmode") in ("require", "verify-ca", "verify-full"):
    use_ssl = True

connect_args = {}
if use_ssl:
    ssl_ctx = ssl.create_default_context()
    # For dev convenience, allow skipping verification controlled by DB_SSL_NO_VERIFY env var.
    # WARNING: disabling verification is insecure for production.
    skip_verify = os.getenv("DB_SSL_NO_VERIFY", "1").strip().lower() not in ("0", "false", "no")
    if skip_verify:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        log.warning("DB SSL verification is DISABLED (DB_SSL_NO_VERIFY=%r). This is insecure for production.",
                    os.getenv("DB_SSL_NO_VERIFY"))
    else:
        ca_file = os.getenv("DB_SSL_CA_FILE")
        if ca_file:
            ssl_ctx.load_verify_locations(cafile=ca_file)
    connect_args["ssl"] = ssl_ctx

log.info("Connecting with async URL (masked): %s  ssl=%s", _mask_dburl(DATABASE_URL_ASYNC), use_ssl)

## Use NullPool in dev to avoid problems when event loop is restarted/closed
from sqlalchemy.pool import NullPool

engine = create_async_engine(
    DATABASE_URL_ASYNC,
    echo=False,
    future=True,
    # Use NullPool to avoid asyncpg trying to terminate pooled connections
    # when the event loop is gone (works well for local dev & reloads).
    poolclass=NullPool,
    connect_args=connect_args or None,
)

# session factory
AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)

# dependency
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session
