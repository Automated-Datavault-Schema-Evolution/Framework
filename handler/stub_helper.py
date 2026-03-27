from handler import lake_handler_client, vault_handler_client


def refresh_stubs() -> tuple:
    return lake_handler_client.create_stub(), vault_handler_client.create_stub()
