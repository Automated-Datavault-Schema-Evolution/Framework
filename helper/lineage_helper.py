import json
from pathlib import Path
from typing import Iterable, Dict, Any, List

from logger import log
from psycopg2._json import Json

from config.config import LAKE_TYPE
from helper.pg_helper import get_metastore_connection, release_metastore_connection


# ----------------- FS -------------------------------------------------------------------
def _fs_root() -> Path:
    root = Path("/opt/sef/lineage")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _fs_edges_path() -> Path:
    root = _fs_root()
    return root / "edges.jsonl"


def _fs_append_edges(edges: Iterable[Dict[str, Any]]):
    """
    Append edges as JSON lines to the edges.jsonl file.
    """
    path = _fs_edges_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        for edge in edges:
            f.write(json.dumps(edge, sort_keys=True) + "\n")


def _fs_load_edges() -> List[Dict[str, Any]]:
    """
    Load all lineage edges from the filesystem store.

    This is intentionally simple and not optimised for very large graphs,
    which is acceptable for the current artefact.
    """
    path = _fs_edges_path()
    if not path.exists():
        return []
    edges: List[Dict[str, Any]] = []
    with path.open() as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                edges.append(json.loads(line))
            except json.JSONDecodeError:
                # skip malformed lines rather than failing whole load
                continue
    return edges


# ----------------- RDBMS -------------------------------------------------------------------
def _db_ensure_schema(conn):
    """
        Ensure that the lineage tables exist.
        """
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS sef_lineage_edges
            (
                id
                BIGSERIAL
                PRIMARY
                KEY,
                upstream_type
                TEXT
                NOT
                NULL,
                upstream_id
                TEXT
                NOT
                NULL,
                downstream_type
                TEXT
                NOT
                NULL,
                downstream_id
                TEXT
                NOT
                NULL,
                via_attributes
                TEXT[]
                NOT
                NULL
                DEFAULT
                '{}',
                metadata_json
                JSONB,
                created_at
                TIMESTAMPTZ
                NOT
                NULL
                DEFAULT (
                NOW
            (
            ) AT TIME ZONE 'UTC')
                );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sef_lineage_upstream
                ON sef_lineage_edges (upstream_id, upstream_type);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_sef_lineage_downstream
                ON sef_lineage_edges (downstream_id, downstream_type);
            """
        )


def _db_insert_edges(edges: Iterable[Dict[str, Any]]):
    conn = get_metastore_connection()
    try:
        _db_ensure_schema(conn)
        rows = [
            (
                edge["upstream_type"],
                edge["upstream_id"],
                edge["downstream_type"],
                edge["downstream_id"],
                edge.get("via_attributes", []),
                Json(edge.get("metadata", []))
            )
            for edge in edges
        ]
        if not rows:
            return
        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO sef_lineage_edges (upstream_type,
                                               upstream_id,
                                               downstream_type,
                                               downstream_id,
                                               via_attributes,
                                               metadata_json)
                VALUES (%s, %s, %s, %s, %s, %s);
                """,
                rows,
            )
    finally:
        release_metastore_connection(conn)


def _db_load_edges() -> List[Dict[str, Any]]:
    """
    Load all lineage edges.

    For now we favour simplicity over a complex recursive CTE and fetch
    the entire edge set into memory, assuming the graph remains modest
    for the present use case.
    """
    conn = get_metastore_connection()
    try:
        _db_ensure_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT upstream_type,
                       upstream_id,
                       downstream_type,
                       downstream_id,
                       via_attributes,
                       metadata_json
                FROM sef_lineage_edges;
                """
            )
            edges: List[Dict[str, Any]] = []
            for (
                    upstream_type,
                    upstream_id,
                    downstream_type,
                    downstream_id,
                    via_attributes,
                    metadata_json,
            ) in cur.fetchall():
                edges.append(
                    {
                        "upstream_type": upstream_type,
                        "upstream_id": upstream_id,
                        "downstream_type": downstream_type,
                        "downstream_id": downstream_id,
                        "via_attributes": list(via_attributes or []),
                        "metadata": metadata_json or {},
                    }
                )
    finally:
        release_metastore_connection(conn)
    return edges


def record_lineage(edges: Iterable[Dict[str, Any]]) -> None:
    """
    Persist one or more lineage edges.

    This function is intentionally forgiving: it ignores an empty iterator and
    does not currently de-duplicate edges. De-duplication can be added later
    if required.
    """
    # Materialise to list once, so we can safely iterate twice if needed
    edge_list = list(edges)
    if not edge_list:
        return

    if LAKE_TYPE == "rdbms":
        return _db_insert_edges(edge_list)
    elif LAKE_TYPE == "parquet":
        return _fs_append_edges(edge_list)
    else:
        log.error(f"[SEF_HELPER][LINEAGE] Unknown lake type: {LAKE_TYPE}")
        return None


def get_impacted_artifacts(dataset_id: str, changed_attributes: List[str]) -> Dict[str, Any]:
    """
    Compute the impacted artefacts for a given dataset and set of changed attributes.

    The algorithm is lineage-aware in two ways:

      * Graph traversal: it follows edges "downstream" starting from the given
        dataset identifier, so it can capture transitive impacts.

      * Attribute filter: if an edge declares via_attributes, it is only
        considered when there is at least one common attribute with the
        changed_attributes set. Edges without via_attributes are considered
        unconditionally.

    :param dataset_id: The identifier of the dataset whose schema changed (typically the dataset-id from the schema
    notification)
    :param changed_attributes:List of attribute/column names that are affected by the schema change.
    :return:
        dict
        {
            "lake_tables": [ "<lake table id>", ... ],
            "vault_objects": [ "<hub/link/satellite id>", ... ],
            "downstream_transformations": [ "<job/view/report id>", ... ],
        }
    """
    if LAKE_TYPE == "rdbms":
        edges = _db_load_edges()
    elif LAKE_TYPE == "parquet":
        edges = _fs_load_edges()
    else:
        log.error(f"[SEF_HELPER][LINEAGE] Unknown lake type: {LAKE_TYPE}")
        return None

    if not edges:
        return {
            "lake_tables": [],
            "vault_objects": [],
            "downstream_transformations": [],
        }

    changed_set = set(changed_attributes or [])

    # Build adjacency list keyed by upstream_id (we ignore upstream_type for reachability)
    from collections import defaultdict, deque

    adjacency: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for edge in edges:
        adjacency[edge["upstream_id"]].append(edge)

    visited: set[str] = set()
    queue: "deque[str]" = deque()

    queue.append(dataset_id)
    visited.add(dataset_id)

    impacted_lake: set[str] = set()
    impacted_vault: set[str] = set()
    impacted_xforms: set[str] = set()

    def _edge_relevant(e: Dict[str, Any]) -> bool:
        via_attrs = e.get("via_attributes") or []
        if not via_attrs:
            return True
        if not changed_set:
            return True
        return bool(changed_set.intersection(via_attrs))

    while queue:
        current = queue.popleft()
        for e in adjacency.get(current, []):
            if not _edge_relevant(e):
                continue

            downstream_id = e["downstream_id"]
            downstream_type = (e.get("downstream_type") or "").lower()

            # Collect impacted artefacts by type
            if downstream_type in ("lake_table", "lake_view", "table", "view"):
                impacted_lake.add(downstream_id)
            elif downstream_type in (
                    "vault_hub",
                    "vault_link",
                    "vault_satellite",
                    "vault_object",
            ):
                impacted_vault.add(downstream_id)
            elif downstream_type in ("transformation", "job", "report", "dashboard"):
                impacted_xforms.add(downstream_id)

            # Continue traversal downstream if not yet visited
            if downstream_id not in visited:
                visited.add(downstream_id)
                queue.append(downstream_id)

    return {
        "lake_tables": sorted(impacted_lake),
        "vault_objects": sorted(impacted_vault),
        "downstream_transformations": sorted(impacted_xforms),
    }
