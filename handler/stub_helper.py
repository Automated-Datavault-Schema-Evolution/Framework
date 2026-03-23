"""gRPC stub factory helpers.

Extracted from main.py to keep the entrypoint thin while preserving behavior.
"""

from handler import lake_handler_client, vault_handler_client


def refresh_stubs() -> tuple:
    """Recreate gRPC stubs (helps after transient network/container restarts)."""
    return lake_handler_client.create_stub(), vault_handler_client.create_stub()
