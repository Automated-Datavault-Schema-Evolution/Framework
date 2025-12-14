from typing import Any, Dict, Optional
from uuid import uuid4

from core.model import SchemaNotification, ChangeContext, HeaderSnapshot


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

    ctx: ChangeContext = {
        "dataset_id": dataset_id,
        "source": notification["dataset"].get("source", ""),
        "correlation_id": correlation_id,
        "previous_schema": previous_header,
        "new_schema": notification["header"],
        "previous_version": notification.get("previous_schema_version"),
        "ingestion_ts": notification["ingestion"]["observed_at"],
        "raw_notification": notification,
    }
    return ctx
