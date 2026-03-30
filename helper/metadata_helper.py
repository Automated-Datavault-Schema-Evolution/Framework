import datetime
import json
from pathlib import Path
from typing import Any, Dict, Optional

from logger import log
from psycopg2 import sql
from psycopg2._json import Json

from config.config import LAKE_TYPE, SEF_METADATA_STORE_RAW, SEF_METASTORE_SCHEMA
from helper.pg_helper import get_metastore_connection, release_metastore_connection


def _normalize_metadata_store(value: str) -> str:
    value = (value or "").strip().lower()
    if value in {"postgres", "pg", "postgre", "postgresql"}:
        return "postgres"
    if value in {"fs", "file", "files", "filesystem"}:
        return "fs"
    return ""


SEF_METADATA_STORE = _normalize_metadata_store(SEF_METADATA_STORE_RAW)


def _effective_metadata_store() -> str:
    if SEF_METADATA_STORE:
        return SEF_METADATA_STORE
    return "postgres" if LAKE_TYPE == "rdbms" else "fs"


def _fs_root() -> Path:
    root = Path("/opt/sef/metadata")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _fs_dataset_dir(dataset_id: str) -> Path:
    safe = dataset_id.replace("/", "_").replace(":", "_")
    dataset_dir = _fs_root() / "datasets" / safe
    dataset_dir.mkdir(parents=True, exist_ok=True)
    return dataset_dir


def _fs_load_latest_schema(dataset_id: str) -> Optional[Dict[str, Any]]:
    index = _fs_dataset_dir(dataset_id) / "schema_index.json"
    if not index.exists():
        return None
    with index.open() as handle:
        return json.load(handle)


def _fs_store_schema_version(dataset_id: str, header: Dict[str, Any], correlation_id: str) -> int:
    dataset_dir = _fs_dataset_dir(dataset_id)
    index = dataset_dir / "schema_index.json"
    if index.exists():
        with index.open() as handle:
            data = json.load(handle)
        version = int(data.get("version", 0)) + 1
    else:
        version = 1

    record = {
        "dataset_id": dataset_id,
        "version": version,
        "header": header,
        "correlation_id": correlation_id,
        "stored_at": datetime.datetime.utcnow().isoformat() + "Z",
    }

    with (dataset_dir / f"schema_{version}.json").open("w") as handle:
        json.dump(record, handle, indent=2)
    with index.open("w") as handle:
        json.dump(record, handle, indent=2)
    return version


def _fs_store_generic(subdir: str, key: str, payload: Dict[str, Any]) -> None:
    directory = _fs_root() / subdir
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / f"{key}.json").open("w") as handle:
        json.dump(payload, handle, indent=2)


def _qname(schema_name: str, table_name: str) -> sql.SQL:
    return sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))


def _ensure_db_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SEF_METASTORE_SCHEMA)))
        cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(SEF_METASTORE_SCHEMA)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    dataset_id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    header_json JSONB NOT NULL,
                    correlation_id TEXT,
                    stored_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (dataset_id, version)
                );
                """
            ).format(_qname(SEF_METASTORE_SCHEMA, "sef_schema_versions"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    plan_id TEXT PRIMARY KEY,
                    dataset_id TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    plan_json JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                );
                """
            ).format(_qname(SEF_METASTORE_SCHEMA, "sef_plans"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    plan_id TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    execution_json JSONB NOT NULL,
                    stored_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (plan_id)
                );
                """
            ).format(_qname(SEF_METASTORE_SCHEMA, "sef_executions"))
        )
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    plan_id TEXT NOT NULL,
                    correlation_id TEXT NOT NULL,
                    verification_json JSONB NOT NULL,
                    stored_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (plan_id)
                );
                """
            ).format(_qname(SEF_METASTORE_SCHEMA, "sef_verifications"))
        )
    conn.commit()


def _db_load_latest_schema(dataset_id: str) -> Optional[Dict[str, Any]]:
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT header_json, version, correlation_id, stored_at
                FROM sef_schema_versions
                WHERE dataset_id = %s
                ORDER BY version DESC LIMIT 1;
                """,
                (dataset_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            header_json, version, correlation_id, stored_at = row
            return {
                "dataset_id": dataset_id,
                "version": int(version),
                "header": header_json,
                "correlation_id": correlation_id,
                "stored_at": stored_at.isoformat().replace("+00:00", "Z"),
            }
    finally:
        release_metastore_connection(conn)


def _db_store_schema_version(dataset_id: str, header: Dict[str, Any], correlation_id: str) -> int:
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COALESCE(MAX(version), 0) FROM sef_schema_versions WHERE dataset_id = %s;",
                (dataset_id,),
            )
            (max_version,) = cur.fetchone()
            version = int(max_version) + 1
            cur.execute(
                """
                INSERT INTO sef_schema_versions (dataset_id, version, header_json, correlation_id, stored_at)
                VALUES (%s, %s, %s, %s, NOW() AT TIME ZONE 'UTC');
                """,
                (dataset_id, version, Json(header), correlation_id),
            )
        conn.commit()
        return version
    finally:
        release_metastore_connection(conn)


def _db_store_plan(plan: Dict[str, Any]) -> None:
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sef_plans (plan_id, dataset_id, correlation_id, plan_json, created_at)
                VALUES (%s, %s, %s, %s, NOW() AT TIME ZONE 'UTC')
                ON CONFLICT (plan_id) DO UPDATE
                    SET dataset_id = EXCLUDED.dataset_id,
                        correlation_id = EXCLUDED.correlation_id,
                        plan_json = EXCLUDED.plan_json,
                        created_at = EXCLUDED.created_at;
                """,
                (
                    plan["plan_id"],
                    plan["dataset_id"],
                    plan["correlation_id"],
                    Json(plan),
                ),
            )
        conn.commit()
    finally:
        release_metastore_connection(conn)


def _db_store_execution_result(result: Dict[str, Any]) -> None:
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sef_executions (plan_id, correlation_id, execution_json, stored_at)
                VALUES (%s, %s, %s, NOW() AT TIME ZONE 'UTC')
                ON CONFLICT (plan_id) DO UPDATE
                    SET correlation_id = EXCLUDED.correlation_id,
                        execution_json = EXCLUDED.execution_json,
                        stored_at = EXCLUDED.stored_at;
                """,
                (result["plan_id"], result["correlation_id"], Json(result)),
            )
        conn.commit()
    finally:
        release_metastore_connection(conn)


def _db_store_verification_result(result: Dict[str, Any]) -> None:
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sef_verifications (plan_id, correlation_id, verification_json, stored_at)
                VALUES (%s, %s, %s, NOW() AT TIME ZONE 'UTC')
                ON CONFLICT (plan_id) DO UPDATE
                    SET correlation_id = EXCLUDED.correlation_id,
                        verification_json = EXCLUDED.verification_json,
                        stored_at = EXCLUDED.stored_at;
                """,
                (result["plan_id"], result["correlation_id"], Json(result)),
            )
        conn.commit()
    finally:
        release_metastore_connection(conn)


def load_latest_schema(dataset_id: str) -> Optional[Dict[str, Any]]:
    store = _effective_metadata_store()
    if store == "postgres":
        return _db_load_latest_schema(dataset_id)
    if store == "fs":
        return _fs_load_latest_schema(dataset_id)
    log.error(f"[SEF_HELPER][METADATA] Unknown metadata store: {store}")
    return None


def store_schema_version(dataset_id: str, header: Dict[str, Any], correlation_id: str) -> int:
    store = _effective_metadata_store()
    if store == "postgres":
        return _db_store_schema_version(dataset_id, header, correlation_id)
    if store == "fs":
        return _fs_store_schema_version(dataset_id, header, correlation_id)
    raise ValueError(f"Unknown metadata store: {store}")


def store_plan(plan: Dict[str, Any]) -> None:
    store = _effective_metadata_store()
    if store == "postgres":
        _db_store_plan(plan)
        return
    if store == "fs":
        _fs_store_generic("plans", plan["plan_id"], plan)
        return
    raise ValueError(f"Unknown metadata store: {store}")


def store_execution_result(result: Dict[str, Any]) -> None:
    store = _effective_metadata_store()
    if store == "postgres":
        _db_store_execution_result(result)
        return
    if store == "fs":
        _fs_store_generic("executions", result["plan_id"], result)
        return
    raise ValueError(f"Unknown metadata store: {store}")


def store_verification_result(result: Dict[str, Any]) -> None:
    store = _effective_metadata_store()
    if store == "postgres":
        _db_store_verification_result(result)
        return
    if store == "fs":
        _fs_store_generic("verifications", result["plan_id"], result)
        return
    raise ValueError(f"Unknown metadata store: {store}")
