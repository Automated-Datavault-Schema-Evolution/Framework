import json


def truncate(s: str, max_chars: int) -> str:
    if max_chars <= 0:
        return s
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + f"... <truncated {len(s) - max_chars} chars>"


def dump_json(obj, max_chars: int) -> str:
    """JSON-dumps an object for debug logging and truncates to max_chars.

    Preserves prior behavior from SchemaEvolutionFramework main.py.
    """
    try:
        s = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        s = repr(obj)
    return truncate(s, max_chars)
