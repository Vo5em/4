import os

from dotenv import load_dotenv
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

load_dotenv()


def validate_database_url(value: str | None) -> str:
    if not value or not value.strip():
        raise RuntimeError(
            "Set DATABASE_URL to a PostgreSQL SQLAlchemy URL using asyncpg."
        )
    try:
        url = make_url(value.strip())
    except ArgumentError:
        raise RuntimeError("DATABASE_URL is not a valid SQLAlchemy URL.") from None
    if url.drivername != "postgresql+asyncpg":
        raise RuntimeError(
            "DATABASE_URL must use the postgresql+asyncpg driver."
        )
    return value.strip()


DATABASE_URL = os.getenv("DATABASE_URL")
try:
    DATABASE_URL = validate_database_url(DATABASE_URL)
except RuntimeError:
    if DATABASE_URL:
        raise
    raise RuntimeError("Set DATABASE_URL in the process environment before starting the application.")


def _positive_int_setting(name: str, default: int) -> int:
    value = os.getenv(name, str(default))
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise RuntimeError(f"{name} must be a positive integer.") from None
    if parsed <= 0:
        raise RuntimeError(f"{name} must be a positive integer.")
    return parsed


# Keep cancellation diagnostics for a month. Active manual suppressions are
# excluded from cleanup regardless of this retention period.
CANCELLED_TRADE_RETENTION_MINUTES = _positive_int_setting(
    "CANCELLED_TRADE_RETENTION_MINUTES", 60
)
CANCELLED_TRADE_CLEANUP_BATCH_SIZE = _positive_int_setting(
    "CANCELLED_TRADE_CLEANUP_BATCH_SIZE", 500
)
CANCELLED_TRADE_CLEANUP_INTERVAL_SECONDS = 60
