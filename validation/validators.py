"""
Validators: run checks and append to ValidationReport. No rows are dropped.
Normalization (dates, channel, cost_micros→spend) is applied in place for downstream use.
"""
from typing import Optional, Tuple

import pandas as pd
from dateutil import parser as date_parser

from .report import ValidationReport


def _parse_date(value) -> Optional[str]:
    """Parse to YYYY-MM-DD or None."""
    if pd.isna(value) or value == "":
        return None
    try:
        return date_parser.parse(str(value)).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def validate_google_ads(
    df: pd.DataFrame,
    report: Optional[ValidationReport] = None,
) -> Tuple[pd.DataFrame, ValidationReport]:
    """
    Validate Google Ads: report nulls, add spend (cost_micros/1e6). No rows dropped.
    """
    report = report or ValidationReport()
    source = "google_ads"
    out = df.copy()

    for idx, row in out.iterrows():
        row_id = f"{row.get('campaign_id')}|{row.get('date')}"
        for col in ["campaign_id", "date", "impressions", "clicks", "cost_micros"]:
            if col in out.columns and pd.isna(row.get(col)):
                report.add(source, row_id, col, "missing", "Null value", value=None)
        if "cost_micros" in out.columns:
            try:
                micros = float(row.get("cost_micros", 0) or 0)
                if micros < 0:
                    report.add(source, row_id, "cost_micros", "invalid", "Negative cost_micros", value=micros)
            except (TypeError, ValueError):
                report.add(source, row_id, "cost_micros", "invalid", "Non-numeric cost_micros", value=row.get("cost_micros"))

    # Convert cost_micros to spend (dollars) for unified layer
    if "cost_micros" in out.columns:
        out["spend"] = (pd.to_numeric(out["cost_micros"], errors="coerce").fillna(0) / 1_000_000).round(2)

    # Duplicate (campaign_id, date)
    dup = out.duplicated(subset=["campaign_id", "date"], keep=False)
    if dup.any():
        for idx in out.index[dup]:
            r = out.loc[idx]
            report.add(source, f"{r['campaign_id']}|{r['date']}", "_key", "duplicate", "Duplicate (campaign_id, date)")

    return out, report


def validate_facebook(
    df: pd.DataFrame,
    report: Optional[ValidationReport] = None,
) -> Tuple[pd.DataFrame, ValidationReport]:
    """
    Validate Facebook: report nulls, date format issues, duplicates. Normalize date to ISO. No rows dropped.
    """
    report = report or ValidationReport()
    source = "facebook"
    out = df.copy()

    # Normalize date and report non-ISO formats
    if "date" in out.columns:
        parsed = []
        for idx, val in out["date"].items():
            iso = _parse_date(val)
            if iso is None and pd.notna(val):
                report.add(source, str(idx), "date", "date_format", f"Unparseable date: {val}", value=val)
            parsed.append(iso if iso else val)
        out["date"] = parsed  # ISO where parse succeeded, else original
        out["date_normalized"] = parsed

    for idx, row in out.iterrows():
        row_id = f"{row.get('campaign_id')}|{row.get('date')}"
        for col in ["campaign_id", "date", "impressions", "clicks", "spend"]:
            if col in out.columns and pd.isna(row.get(col)):
                report.add(source, row_id, col, "missing", "Null value", value=None)
        if "purchases" in out.columns and pd.isna(row.get("purchases")):
            report.add(source, row_id, "purchases", "missing", "Null purchases", value=None)

    dup = out.duplicated(subset=["campaign_id", "date"], keep=False)
    if dup.any():
        for idx in out.index[dup]:
            r = out.loc[idx]
            report.add(source, f"{r['campaign_id']}|{r['date']}", "_key", "duplicate", "Duplicate (campaign_id, date)")

    return out, report


def validate_crm(
    df: pd.DataFrame,
    report: Optional[ValidationReport] = None,
) -> Tuple[pd.DataFrame, ValidationReport]:
    """
    Validate CRM: report nulls, negative revenue, outliers, duplicates, channel casing, empty campaign_source.
    Normalize order_date to ISO and channel_attributed to lowercase. No rows dropped.
    """
    report = report or ValidationReport()
    source = "crm"
    out = df.copy()

    # Normalize order_date to ISO (YYYY-MM-DD); correct main column and keep _normalized for reference
    if "order_date" in out.columns:
        parsed = []
        for idx, val in out["order_date"].items():
            iso = _parse_date(val)
            if iso is None and pd.notna(val):
                report.add(source, out.loc[idx, "order_id"] if "order_id" in out.columns else str(idx), "order_date", "date_format", f"Unparseable date: {val}", value=val)
            parsed.append(iso if iso else val)
        out["order_date"] = parsed
        out["order_date_normalized"] = parsed

    # Normalize channel_attributed to lowercase; report inconsistent casing
    if "channel_attributed" in out.columns:
        raw = out["channel_attributed"].astype(str).str.strip()
        lower = raw.str.lower()
        out["channel_attributed_normalized"] = lower
        for idx in out.index:
            rw, lw = raw.loc[idx], lower.loc[idx]
            if rw not in ("nan", "") and rw != lw:
                report.add(source, out.loc[idx, "order_id"], "channel_attributed", "inconsistent_casing", f"Value '{rw}' normalized to '{lw}'", value=rw)

    for idx, row in out.iterrows():
        oid = row.get("order_id", str(idx))
        cust = row.get("customer_id")
        if pd.isna(cust) or (isinstance(cust, str) and cust.strip() == ""):
            report.add(source, oid, "customer_id", "missing", "Null or empty customer_id", value=cust)
        if pd.isna(row.get("revenue")):
            report.add(source, oid, "revenue", "missing", "Null revenue", value=None)
        else:
            try:
                rev = float(row["revenue"])
                if rev < 0:
                    report.add(source, oid, "revenue", "invalid", "Negative revenue", value=rev)
            except (TypeError, ValueError):
                report.add(source, oid, "revenue", "invalid", "Non-numeric revenue", value=row.get("revenue"))
        camp = row.get("campaign_source")
        if pd.isna(camp) or (isinstance(camp, str) and camp.strip() == ""):
            report.add(source, oid, "campaign_source", "missing", "Null or empty campaign_source", value=camp)

    # Outliers: record during ingestion (revenue > 10x p99, or > 1e6 absolute)
    if "revenue" in out.columns:
        rev = pd.to_numeric(out["revenue"], errors="coerce")
        valid = rev.dropna()
        if len(valid) > 0:
            p99 = valid.quantile(0.99)
            threshold_p99 = p99 * 10
            for idx in out.index:
                val = rev.loc[idx]
                if pd.isna(val):
                    continue
                val_f = float(val)
                if val_f > 1_000_000:
                    report.add(source, out.loc[idx, "order_id"], "revenue", "outlier", f"Revenue > 1e6 (absolute)", value=val_f)
                elif val_f > threshold_p99:
                    report.add(source, out.loc[idx, "order_id"], "revenue", "outlier", f"Revenue > {threshold_p99:.2f} (10x p99={p99:.2f})", value=val_f)

    # Duplicate order_id: report each duplicate (don't drop)
    dup = out.duplicated(subset=["order_id"], keep=False)
    if dup.any():
        for idx in out.index[dup]:
            report.add(source, out.loc[idx, "order_id"], "order_id", "duplicate", "Duplicate order_id")

    return out, report
