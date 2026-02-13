"""
Load validated DataFrames into the warehouse.
Uses normalized date/channel columns where present. All rows are loaded (no drop).
"""
import pandas as pd
from sqlalchemy.engine import Engine

from .schema import create_warehouse_schema, get_engine


def load_from_validated(
    engine: Engine,
    df_google: pd.DataFrame,
    df_facebook: pd.DataFrame,
    df_crm: pd.DataFrame,
) -> None:
    """
    Full refresh: create schema, then load dim_campaign, fact_ad_performance, fact_orders.
    """
    create_warehouse_schema(engine)

    # pandas to_sql expects a DB-API connection with .cursor(); SQLAlchemy Connection doesn't have it.
    # Use the raw connection from the pool proxy (conn.connection -> dbapi_connection).
    with engine.begin() as conn:
        raw = getattr(conn, "connection", conn)
        raw = getattr(raw, "dbapi_connection", raw)
        # ---- dim_campaign (distinct campaigns from Google + Facebook) ----
        cols_google = ["campaign_id", "platform", "campaign_name", "campaign_type", "status"]
        dim_google = df_google[cols_google].drop_duplicates(subset=["campaign_id", "platform"], keep="first")

        dim_facebook = df_facebook[["campaign_id", "platform", "campaign_name"]].drop_duplicates(
            subset=["campaign_id", "platform"], keep="first"
        )
        dim_facebook["campaign_type"] = None
        dim_facebook["status"] = None
        dim_facebook = dim_facebook[cols_google]

        dim_campaign = pd.concat([dim_google, dim_facebook], ignore_index=True).drop_duplicates(
            subset=["campaign_id", "platform"], keep="first"
        )
        dim_campaign.to_sql("dim_campaign", raw, if_exists="append", index=False, method="multi")

        # ---- fact_ad_performance: Google ----
        goog = df_google.copy()
        goog["date"] = pd.to_datetime(goog["date"], errors="coerce")
        goog = goog.dropna(subset=["date"])
        for c in ["impressions", "clicks", "spend", "conversions", "conversion_value"]:
            if c in goog.columns:
                goog[c] = pd.to_numeric(goog[c], errors="coerce").fillna(0)
        goog["reach"] = None
        goog["frequency"] = None
        fact_cols = ["date", "campaign_id", "platform", "impressions", "clicks", "spend", "conversions", "conversion_value", "reach", "frequency"]
        goog[fact_cols].to_sql("fact_ad_performance", raw, if_exists="append", index=False, method="multi")

        # ---- fact_ad_performance: Facebook (purchases -> conversions, purchase_value -> conversion_value) ----
        fb = df_facebook.copy()
        fb["date"] = pd.to_datetime(fb["date"], errors="coerce")
        fb = fb.dropna(subset=["date"])
        fb["conversions"] = (fb["purchases"] if "purchases" in fb.columns else 0).fillna(0).astype(int)
        fb["conversion_value"] = (fb["purchase_value"] if "purchase_value" in fb.columns else 0).fillna(0)
        fb["reach"] = fb["reach"] if "reach" in fb.columns else None
        fb["frequency"] = fb["frequency"] if "frequency" in fb.columns else None
        fb[fact_cols].to_sql("fact_ad_performance", raw, if_exists="append", index=False, method="multi")

        # ---- fact_orders (use normalized channel if present) ----
        crm = df_crm.copy()
        crm["order_date"] = pd.to_datetime(crm["order_date"], errors="coerce")
        crm = crm.dropna(subset=["order_date"])
        # PK on order_id: keep first of duplicates (duplicates already in validation report)
        crm = crm.drop_duplicates(subset=["order_id"], keep="first")
        if "channel_attributed_normalized" in crm.columns:
            crm["channel_attributed"] = crm["channel_attributed_normalized"]
        crm["revenue"] = pd.to_numeric(crm["revenue"], errors="coerce")
        order_cols = ["order_id", "customer_id", "order_date", "revenue", "channel_attributed", "campaign_source", "product_category", "region"]
        crm[order_cols].to_sql("fact_orders", raw, if_exists="append", index=False, method="multi")
