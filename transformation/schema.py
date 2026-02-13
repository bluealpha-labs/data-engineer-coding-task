"""
Warehouse schema: normalized tables for analytics.
- dim_campaign: one row per campaign per platform (supports multiple attribution models).
- fact_ad_performance: daily ad metrics from Google + Facebook.
- fact_orders: order-level revenue with attribution (raw fields kept for flexibility).
"""
from pathlib import Path
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Default: Postgres from docker-compose
_ROOT = Path(__file__).resolve().parent.parent


def get_engine(url: Optional[str] = None) -> Engine:
    import os
    u = url or os.environ.get("WAREHOUSE_URL", "")
    if not u:
        # Default: SQLite in output/ so the pipeline runs without Docker
        _root = Path(__file__).resolve().parent.parent
        db_path = _root / "output" / "warehouse.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        u = f"sqlite:///{db_path}"
    return create_engine(u)


def create_warehouse_schema(engine: Engine) -> None:
    """Create warehouse tables. Idempotent: drops and recreates (full refresh)."""
    is_sqlite = "sqlite" in str(engine.url)
    drop_cascade = "" if is_sqlite else " CASCADE"

    # SQLAlchemy 2: use begin() so the transaction commits on exit (no .commit() on Connection)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS fact_orders{drop_cascade}"))
        conn.execute(text(f"DROP TABLE IF EXISTS fact_ad_performance{drop_cascade}"))
        conn.execute(text(f"DROP TABLE IF EXISTS dim_campaign{drop_cascade}"))

    # SQLite uses INTEGER PRIMARY KEY AUTOINCREMENT; Postgres uses SERIAL
    id_col = "id INTEGER PRIMARY KEY AUTOINCREMENT" if is_sqlite else "id SERIAL PRIMARY KEY"

    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE dim_campaign (
                campaign_id   VARCHAR(64) NOT NULL,
                platform     VARCHAR(32) NOT NULL,
                campaign_name VARCHAR(256),
                campaign_type VARCHAR(64),
                status       VARCHAR(32),
                PRIMARY KEY (campaign_id, platform)
            )
        """))
        conn.execute(text(f"""
            CREATE TABLE fact_ad_performance (
                {id_col},
                date             DATE NOT NULL,
                campaign_id      VARCHAR(64) NOT NULL,
                platform         VARCHAR(32) NOT NULL,
                impressions      BIGINT NOT NULL DEFAULT 0,
                clicks          INTEGER NOT NULL DEFAULT 0,
                spend           NUMERIC(14,2) NOT NULL DEFAULT 0,
                conversions     INTEGER NOT NULL DEFAULT 0,
                conversion_value NUMERIC(14,2) NOT NULL DEFAULT 0,
                reach           BIGINT,
                frequency       NUMERIC(6,2)
            )
        """))
        conn.execute(text("""
            CREATE TABLE fact_orders (
                order_id           VARCHAR(32) PRIMARY KEY,
                customer_id        VARCHAR(32),
                order_date         DATE NOT NULL,
                revenue            NUMERIC(14,2),
                channel_attributed VARCHAR(32),
                campaign_source    VARCHAR(64),
                product_category  VARCHAR(128),
                region             VARCHAR(64)
            )
        """))
