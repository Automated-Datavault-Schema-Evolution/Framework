from typing import Any, Dict, Optional
from uuid import uuid4

from config.config import TECHNICAL_COLUMNS
from core.model import ChangeContext, HeaderSnapshot, SchemaNotification


_TECHNICAL_COLUMNS = set(TECHNICAL_COLUMNS)


def _strip_technical_columns(header: Optional[HeaderSnapshot]) -> Optional[HeaderSnapshot]:
    if not header:
        return header

    attributes = header.get("attributes") or []
    if not isinstance(attributes, list) or not attributes:
        return header

    filtered = [
        attribute
        for attribute in attributes
        if isinstance(attribute, dict) and attribute.get("name") not in _TECHNICAL_COLUMNS
    ]
    if len(filtered) == len(attributes):
        return header

    output: HeaderSnapshot = dict(header)
    output["attributes"] = filtered
    return output


def validate_notification(notification: Dict[str, Any]) -> None:
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
    return {
        "dataset_id": notification["dataset"]["id"],
        "source": notification["dataset"].get("source", ""),
        "correlation_id": str(uuid4()),
        "previous_schema": _strip_technical_columns(previous_header),
        "new_schema": _strip_technical_columns(notification["header"]),
        "previous_version": notification.get("previous_schema_version"),
        "ingestion_ts": notification["ingestion"]["observed_at"],
        "raw_notification": notification,
    }
