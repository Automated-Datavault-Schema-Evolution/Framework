from __future__ import annotations

import os
from typing import Any, Dict, Optional
from uuid import uuid4

from core.model import ChangeContext, HeaderSnapshot, SchemaNotification

# Platform-managed columns that must not participate in compatibility decisions.
_TECHNICAL_COLUMNS = {
    c.strip()
    for c in os.getenv("SEF_TECHNICAL_COLUMNS", "ingestion_timestamp").split(",")
    if c.strip()
}


def _strip_technical_columns(header: Optional[HeaderSnapshot]) -> Optional[HeaderSnapshot]:
    if not header:
        return header
    attrs = header.get("attributes") or []
    if not isinstance(attrs, list) or not attrs:
        return header

    filtered = [a for a in attrs if isinstance(a, dict) and a.get("name") not in _TECHNICAL_COLUMNS]
    if len(filtered) == len(attrs):
        return header

    out: HeaderSnapshot = dict(header)
    out["attributes"] = filtered
    return out


def validate_notification(notification: Dict[str, Any]):
    if notification.get("event_type") != "schema.notification":
        raise ValueError("Unsupported event_type")
    if "dataset" not in notification or "id" not in notification["dataset"]:
        raise ValueError("Missing dataset.id")
    if "header" not in notification or "attributes" not in notification["header"]:
        raise ValueError("Missing header.attributes")
    if "ingestion" not in notification or "observed_at" not in notification["ingestion"]:
        raise ValueError("Missing ingestion.observed_at")


def build_change_context(
        notification: SchemaNotification,
        previous_header: Optional[HeaderSnapshot],
) -> ChangeContext:
    dataset_id = notification["dataset"]["id"]
    correlation_id = str(uuid4())

    clean_prev = _strip_technical_columns(previous_header)
    clean_new = _strip_technical_columns(notification["header"])

    ctx: ChangeContext = {
        "dataset_id": dataset_id,
        "source": notification["dataset"].get("source", ""),
        "correlation_id": correlation_id,
        "previous_schema": clean_prev,
        "new_schema": clean_new,
        "previous_version": notification.get("previous_schema_version"),
        "ingestion_ts": notification["ingestion"]["observed_at"],
        "raw_notification": notification,
    }
    return ctx
