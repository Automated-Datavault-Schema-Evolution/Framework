import os


def env_flag(name: str, default: bool = False) -> bool:
    """Parse common truthy env var values.

    Preserves prior behavior from SchemaEvolutionFramework main.py.
    """
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}
