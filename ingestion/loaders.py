"""
Data ingestion layer: load the three challenge sources into pandas DataFrames.
Paths are relative to project root; override with path= for tests.
"""
import json
from pathlib import Path
from typing import Optional

import pandas as pd

# Default paths (project root = parent of ingestion/)
_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = _ROOT / "data"


def load_google_ads(path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load Google Ads API-style JSON and flatten to one row per campaign per date.
    """
    p = path or (DATA_DIR / "google_ads_api.json")
    with open(p) as f:
        data = json.load(f)

    rows = []
    for camp in data.get("campaigns", []):
        for m in camp.get("daily_metrics", []):
            rows.append({
                "campaign_id": camp.get("campaign_id"),
                "campaign_name": camp.get("campaign_name"),
                "campaign_type": camp.get("campaign_type"),
                "status": camp.get("status"),
                "date": m.get("date"),
                "impressions": m.get("impressions"),
                "clicks": m.get("clicks"),
                "cost_micros": m.get("cost_micros"),
                "conversions": m.get("conversions"),
                "conversion_value": m.get("conversion_value"),
            })
    df = pd.DataFrame(rows)
    df["platform"] = "google_ads"
    return df


def load_facebook(path: Optional[Path] = None) -> pd.DataFrame:
    """Load Facebook export CSV."""
    p = path or (DATA_DIR / "facebook_export.csv")
    df = pd.read_csv(p)
    df["platform"] = "facebook"
    return df


def load_crm(path: Optional[Path] = None) -> pd.DataFrame:
    """
    Load CRM revenue CSV. Keeps ALL rows: lines with unquoted commas in the date
    (e.g. "January 4, 2024") are read by merging the extra field into order_date.
    """
    p = Path(path or DATA_DIR / "crm_revenue.csv")
    with open(p, encoding="utf-8", errors="replace") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    if not lines:
        return pd.DataFrame()

    header = [c.strip() for c in lines[0].split(",")]
    expected_cols = len(header)  # 8

    rows = []
    for line in lines[1:]:
        parts = line.split(",")
        if len(parts) == expected_cols:
            rows.append(parts)
        elif len(parts) == expected_cols + 1:
            # Extra comma in date: e.g. order_id, customer_id, "January 4", " 2024", revenue, ...
            # Merge parts[2] and parts[3] into one order_date
            order_date_merged = (parts[2] + "," + parts[3]).strip()
            row = parts[:2] + [order_date_merged] + parts[4:]
            rows.append(row)
        else:
            # Keep row: pad or truncate to expected_cols so we never drop data
            if len(parts) < expected_cols:
                row = parts + [""] * (expected_cols - len(parts))
            else:
                row = parts[:expected_cols]
            rows.append(row)

    df = pd.DataFrame(rows, columns=header)
    return df
