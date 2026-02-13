"""
Validation report: collect issues without dropping rows.
Each entry has source, row_id, column, issue_type, message, value (optional).
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd


class ValidationReport:
    """Mutable list of validation issues. Can be exported to DataFrame/CSV."""

    def __init__(self) -> None:
        self.entries: List[Dict[str, Any]] = []

    def add(
        self,
        source: str,
        row_id: str,
        column: str,
        issue_type: str,
        message: str,
        value: Any = None,
    ) -> None:
        self.entries.append({
            "source": source,
            "row_id": row_id,
            "column": column,
            "issue_type": issue_type,
            "message": message,
            "value": value,
        })

    def to_dicts(self) -> List[Dict[str, Any]]:
        return list(self.entries)

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.entries)

    def save_csv(self, path: Union[str, Path]) -> None:
        self.to_dataframe().to_csv(path, index=False)

    def __len__(self) -> int:
        return len(self.entries)
