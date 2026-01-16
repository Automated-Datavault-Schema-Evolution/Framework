# apps/SchemaEvolutionFramework/helper/metadata_helper.py

import datetime
import json
import os
from pathlib import Path
from typing import Optional, Dict, Any

from logger import log
from psycopg2 import sql
from psycopg2._json import Json

from config.config import LAKE_TYPE
from helper.pg_helper import get_metastore_connection, release_metastore_connection


# ----------------- FS -------------------------------------------------------------------
def _fs_root() -> Path:
    root = Path("/opt/sef/metadata")
    root.mkdir(parents=True, exist_ok=True)
    return root


def _fs_dataset_dir(dataset_id: str) -> Path:
    safe = dataset_id.replace("/", "_").replace(":", "_")
    d = _fs_root() / "datasets" / safe
    d.mkdir(parents=True, exist_ok=True)
    return d


def _fs_load_latest_schema(dataset_id: str) -> Optional[Dict[str, Any]]:
    d = _fs_dataset_dir(dataset_id)
    index = d / "schema_index.json"
    if not index.exists():
        return None
    with index.open() as f:
        return json.load(f)


def _fs_store_schema_version(dataset_id: str, header: Dict[str, Any], correlation_id: str) -> int:
    d = _fs_dataset_dir(dataset_id)
    index = d / "schema_index.json"
    if index.exists():
        with index.open() as f:
            data = json.load(f)
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

    with (d / f"schema_{version}.json").open("w") as f:
        json.dump(record, f, indent=2)
    with index.open("w") as f:
        json.dump(record, f, indent=2)

    return version


def _fs_store_generic(subdir: str, key: str, payload: Dict[str, Any]):
    d = _fs_root() / subdir
    d.mkdir(parents=True, exist_ok=True)
    with (d / f"{key}.json").open("w") as f:
        json.dump(payload, f, indent=2)


# ----------------- RDBMS -------------------------------------------------------------------
SEF_METASTORE_SCHEMA = os.getenv("SEF_METASTORE_SCHEMA", "metastore")


def _qname(schema_name: str, table_name: str) -> sql.SQL:
    return sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))


def _ensure_db_schema(conn) -> None:
    schema = SEF_METASTORE_SCHEMA
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(sql.SQL("SET search_path TO {}, public").format(sql.Identifier(schema)))

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    dataset_id
                    TEXT
                    NOT
                    NULL,
                    version
                    INTEGER
                    NOT
                    NULL,
                    header_json
                    JSONB
                    NOT
                    NULL,
                    correlation_id
                    TEXT,
                    stored_at
                    TIMESTAMPTZ
                    NOT
                    NULL,
                    PRIMARY
                    KEY
                (
                    dataset_id,
                    version
                )
                    );
                """
            ).format(_qname(schema, "sef_schema_versions"))
        )

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    plan_id
                    TEXT
                    PRIMARY
                    KEY,
                    dataset_id
                    TEXT
                    NOT
                    NULL,
                    correlation_id
                    TEXT
                    NOT
                    NULL,
                    plan_json
                    JSONB
                    NOT
                    NULL,
                    created_at
                    TIMESTAMPTZ
                    NOT
                    NULL
                );
                """
            ).format(_qname(schema, "sef_plans"))
        )

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    plan_id
                    TEXT
                    NOT
                    NULL,
                    correlation_id
                    TEXT
                    NOT
                    NULL,
                    execution_json
                    JSONB
                    NOT
                    NULL,
                    stored_at
                    TIMESTAMPTZ
                    NOT
                    NULL,
                    PRIMARY
                    KEY
                (
                    plan_id
                )
                    );
                """
            ).format(_qname(schema, "sef_executions"))
        )

        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}
                (
                    plan_id
                    TEXT
                    NOT
                    NULL,
                    correlation_id
                    TEXT
                    NOT
                    NULL,
                    verification_json
                    JSONB
                    NOT
                    NULL,
                    stored_at
                    TIMESTAMPTZ
                    NOT
                    NULL,
                    PRIMARY
                    KEY
                (
                    plan_id
                )
                    );
                """
            ).format(_qname(schema, "sef_verifications"))
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
    except Exception:
        conn.rollback()
        raise
    finally:
        release_metastore_connection(conn)


def _db_store_plan(plan: Dict[str, Any]):
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sef_plans (plan_id, dataset_id, correlation_id, plan_json, created_at)
                VALUES (%s, %s, %s, %s, NOW() AT TIME ZONE 'UTC') ON CONFLICT (plan_id) DO
                UPDATE
                    SET plan_json = EXCLUDED.plan_json;
                """,
                (plan["plan_id"], plan.get("dataset_id"), plan.get("correlation_id"), Json(plan)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_metastore_connection(conn)


def _db_store_execution_results(result: Dict[str, Any]):
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sef_executions (plan_id, correlation_id, execution_json, stored_at)
                VALUES (%s, %s, %s, NOW() AT TIME ZONE 'UTC') ON CONFLICT (plan_id) DO
                UPDATE
                    SET execution_json = EXCLUDED.execution_json;
                """,
                (result["plan_id"], result.get("correlation_id"), Json(result)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_metastore_connection(conn)


def _db_store_verification_result(verification: Dict[str, Any]):
    conn = get_metastore_connection()
    try:
        _ensure_db_schema(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sef_verifications (plan_id, correlation_id, verification_json, stored_at)
                VALUES (%s, %s, %s, NOW() AT TIME ZONE 'UTC') ON CONFLICT (plan_id) DO
                UPDATE
                    SET verification_json = EXCLUDED.verification_json;
                """,
                (verification["plan_id"], verification.get("correlation_id"), Json(verification)),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        release_metastore_connection(conn)


def load_latest_schema(dataset_id: str) -> Optional[Dict[str, Any]]:
    if LAKE_TYPE == "rdbms":
        return _db_load_latest_schema(dataset_id)
    elif LAKE_TYPE == "parquet":
        return _fs_load_latest_schema(dataset_id)
    log.error(f"[METASTORE] Unknown lake type: {LAKE_TYPE}")
    return None


def store_schema_version(dataset_id: str, header: Dict[str, Any], correlation_id: str) -> int:
    if LAKE_TYPE == "rdbms":
        return _db_store_schema_version(dataset_id, header, correlation_id)
    elif LAKE_TYPE == "parquet":
        return _fs_store_schema_version(dataset_id, header, correlation_id)
    log.error(f"[METASTORE] Unknown lake type: {LAKE_TYPE}")
    raise RuntimeError(f"Unknown lake type: {LAKE_TYPE}")


def store_execution_result(result: Dict[str, Any]):
    if LAKE_TYPE == "rdbms":
        return _db_store_execution_results(result)
    elif LAKE_TYPE == "parquet":
        return _fs_store_generic(subdir="executions", key=result["plan_id"], payload=result)
    log.error(f"[METASTORE] Unknown lake type: {LAKE_TYPE}")
    raise RuntimeError(f"Unknown lake type: {LAKE_TYPE}")


def store_plan(plan: Dict[str, Any]):
    if LAKE_TYPE == "rdbms":
        return _db_store_plan(plan)
    elif LAKE_TYPE == "parquet":
        return _fs_store_generic(subdir="plan", key=plan["plan_id"], payload=plan)
    log.error(f"[METASTORE] Unknown lake type: {LAKE_TYPE}")
    raise RuntimeError(f"Unknown lake type: {LAKE_TYPE}")


def store_verification_result(verification: Dict[str, Any]):
    if LAKE_TYPE == "rdbms":
        return _db_store_verification_result(verification)
    elif LAKE_TYPE == "parquet":
        return _fs_store_generic(subdir="verification", key=verification["plan_id"], payload=verification)
    log.error(f"[METASTORE] Unknown lake type: {LAKE_TYPE}")
    raise RuntimeError(f"Unknown lake type: {LAKE_TYPE}")
