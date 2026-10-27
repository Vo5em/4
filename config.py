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
