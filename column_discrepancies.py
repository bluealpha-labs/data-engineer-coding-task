"""
Column-by-column discrepancy report for the three pipeline datasets.
Run from project root: python column_discrepancies.py
Uses ingestion loaders to load data, then checks each column for issues.
"""
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

# project root
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

import pandas as pd
from ingestion.loaders import load_google_ads, load_facebook, load_crm


def check_nulls(df: pd.DataFrame) -> Dict[str, int]:
    """Null count per column."""
    return df.isnull().sum().to_dict()


def check_duplicates(df: pd.DataFrame, keys: Optional[List[str]]) -> int:
    """Number of duplicate rows by key columns."""
    if not keys or not all(k in df.columns for k in keys):
        return 0
    return int(df.duplicated(subset=keys).sum())


def check_date_formats(series: pd.Series) -> Dict[str, int]:
    """Count distinct string representations (for date columns)."""
    if series.isnull().all():
        return {}
    as_str = series.astype(str)
    return as_str.value_counts(dropna=False).to_dict()


def check_numeric_bounds(series: pd.Series, min_val: Optional[float] = None, max_val: Optional[float] = None) -> List[str]:
    """Check for values outside expected bounds. Returns list of issue descriptions."""
    issues = []
    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if len(valid) == 0:
        return issues
    if min_val is not None and (valid < min_val).any():
        n = (valid < min_val).sum()
        examples = valid[valid < min_val].head(3).tolist()
        issues.append(f"{n} value(s) < {min_val} (e.g. {examples})")
    if max_val is not None and (valid > max_val).any():
        n = (valid > max_val).sum()
        examples = valid[valid > max_val].head(3).tolist()
        issues.append(f"{n} value(s) > {max_val} (e.g. {examples})")
    return issues


def check_categorical_values(series: pd.Series, expected: Optional[Set[str]] = None) -> List[str]:
    """Check for unexpected or inconsistent categorical values (e.g. casing)."""
    issues = []
    uniq = series.dropna().astype(str).str.strip().unique().tolist()
    if expected and uniq:
        lower_uniq = {s.lower() for s in uniq}
        if len(lower_uniq) < len(uniq) or (expected and not lower_uniq.issubset({e.lower() for e in expected})):
            issues.append(f"Values: {uniq} (check casing/expected set)")
    elif len(uniq) <= 20:
        issues.append(f"Values: {uniq}")
    return issues


def run_google_checks(df: pd.DataFrame) -> Dict:
    """Run all checks for Google Ads (flattened) dataframe."""
    report = {}
    for col in df.columns:
        issues = []
        nulls = df[col].isnull().sum()
        if nulls > 0:
            issues.append(f"Nulls: {int(nulls)}")
        if col == "cost_micros":
            issues.append("Unit: micros (convert to dollars in pipeline)")
        if col == "date":
            formats = check_date_formats(df[col])
            if len(formats) > 1:
                issues.append(f"Date formats: {len(formats)} distinct (e.g. {list(formats.keys())[:5]})")
            else:
                issues.append("Date format: single format (OK)")
        report[col] = issues if issues else ["OK"]
    dup_key = ["campaign_id", "date"]
    report["_duplicates"] = [f"Duplicate ({', '.join(dup_key)}): {check_duplicates(df, dup_key)}"]
    return report


def run_facebook_checks(df: pd.DataFrame) -> Dict:
    """Run all checks for Facebook dataframe."""
    report = {}
    for col in df.columns:
        issues = []
        nulls = df[col].isnull().sum()
        if nulls > 0:
            issues.append(f"Nulls: {int(nulls)}")
        if col == "date":
            formats = check_date_formats(df[col])
            if len(formats) > 1:
                issues.append(f"Date formats: {len(formats)} distinct")
                for fmt, count in list(formats.items())[:8]:
                    issues.append(f"  - '{fmt}': {count}")
            else:
                issues.append("Date format: single format (OK)")
        if col in ("purchases", "purchase_value") and nulls > 0:
            issues.append(f"Missing in {nulls} row(s)")
        report[col] = issues if issues else ["OK"]
    dup_key = ["campaign_id", "date"]
    report["_duplicates"] = [f"Duplicate ({', '.join(dup_key)}): {check_duplicates(df, dup_key)}"]
    return report


def run_crm_checks(df: pd.DataFrame) -> Dict:
    """Run all checks for CRM dataframe."""
    report = {}
    for col in df.columns:
        issues = []
        nulls = df[col].isnull().sum()
        if nulls > 0:
            issues.append(f"Nulls: {int(nulls)}")
        if col == "order_date":
            formats = check_date_formats(df[col])
            if len(formats) > 1:
                issues.append(f"Date formats: {len(formats)} distinct")
                for fmt, count in list(formats.items())[:8]:
                    issues.append(f"  - '{fmt}': {count}")
        if col == "revenue":
            issues.extend(check_numeric_bounds(df[col], min_val=0, max_val=None))
            numeric = pd.to_numeric(df[col], errors="coerce")
            if numeric.notna().any():
                q99 = numeric.quantile(0.99)
                outliers = (numeric > q99 * 10).sum()
                if outliers > 0:
                    issues.append(f"Possible outliers (>{q99:.0f}*10): {int(outliers)}")
        if col == "channel_attributed":
            issues.extend(check_categorical_values(df[col], expected={"google", "facebook"}))
        if col == "campaign_source" and nulls > 0:
            issues.append(f"Empty in {int(nulls)} row(s) (breaks join to ad data)")
        if col == "order_id":
            dup = check_duplicates(df, ["order_id"])
            if dup > 0:
                issues.append(f"Duplicate order_id: {dup} duplicate row(s)")
        report[col] = issues if issues else ["OK"]
    report["_duplicates"] = [f"Duplicate (order_id): {check_duplicates(df, ['order_id'])}"]
    return report


def print_report(name: str, report: Dict, key_cols: Optional[List[str]] = None):
    """Print column-by-column report."""
    print(f"\n{'='*60}\n{name}\n{'='*60}")
    for col in [c for c in report if not c.startswith("_")]:
        print(f"\n  {col}")
        for line in report[col]:
            print(f"    - {line}")
    if "_duplicates" in report:
        print("\n  [Key duplicates]")
        for line in report["_duplicates"]:
            print(f"    - {line}")


def main():
    print("Loading data...")
    df_google = load_google_ads()
    df_facebook = load_facebook()
    df_crm = load_crm()

    print("\nColumn-by-column discrepancy report")
    print_report("1. Google Ads (flattened)", run_google_checks(df_google))
    print_report("2. Facebook", run_facebook_checks(df_facebook))
    print_report("3. CRM", run_crm_checks(df_crm))
    print("\nDone.")


if __name__ == "__main__":
    main()
