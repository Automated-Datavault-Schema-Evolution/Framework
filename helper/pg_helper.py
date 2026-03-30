import psycopg2
from logger import log
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool

from config.config import (
    METASTORE_DB,
    METASTORE_DB_HOST,
    METASTORE_DB_PASSWORD,
    METASTORE_DB_PORT,
    METASTORE_DB_USER,
    METASTORE_POOL_MAX,
    METASTORE_POOL_MIN,
)


METASTORE_POOL = None


def _ensure_metastore_db() -> None:
    try:
        conn = psycopg2.connect(
            host=METASTORE_DB_HOST,
            port=METASTORE_DB_PORT,
            dbname=METASTORE_DB,
            user=METASTORE_DB_USER,
            password=METASTORE_DB_PASSWORD,
        )
        conn.close()
        log.debug("Metastore database exists.")
    except psycopg2.OperationalError as exc:
        if "does not exist" not in str(exc):
            log.error(f"Error connecting to metastore database: {exc}")
            raise

        log.info(f"Metastore database '{METASTORE_DB}' missing. Creating it.")
        bootstrap_conn = None
        try:
            bootstrap_conn = psycopg2.connect(
                host=METASTORE_DB_HOST,
                port=METASTORE_DB_PORT,
                dbname="postgres",
                user=METASTORE_DB_USER,
                password=METASTORE_DB_PASSWORD,
            )
            bootstrap_conn.autocommit = True
            with bootstrap_conn.cursor() as cur:
                cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(METASTORE_DB)))
            log.info(f"Metastore database '{METASTORE_DB}' created.")
        except Exception as create_exc:
            log.error(f"Failed creating metastore database '{METASTORE_DB}': {create_exc}")
            raise
        finally:
            if bootstrap_conn:
                bootstrap_conn.close()


def init_metastore_pool(minconn=None, maxconn=None) -> SimpleConnectionPool:
    global METASTORE_POOL
    if minconn is None:
        minconn = METASTORE_POOL_MIN
    if maxconn is None:
        maxconn = METASTORE_POOL_MAX

    if METASTORE_POOL is None:
        _ensure_metastore_db()
        try:
            METASTORE_POOL = SimpleConnectionPool(
                minconn,
                maxconn,
                host=METASTORE_DB_HOST,
                port=METASTORE_DB_PORT,
                dbname=METASTORE_DB,
                user=METASTORE_DB_USER,
                password=METASTORE_DB_PASSWORD,
            )
            log.info(
                f"[SEF_HELPER][METASTORE][RDBMS] Metastore connection pool created "
                f"(min={minconn}, max={maxconn})."
            )
        except Exception as exc:
            log.error(f"[SEF_HELPER][METASTORE][RDBMS] Error establishing metastore connection pool: {exc}")
            raise
    else:
        log.debug("[SEF_HELPER][METASTORE][RDBMS] Reusing existing metastore connection pool.")
    return METASTORE_POOL


def get_metastore_connection():
    pool = init_metastore_pool()
    try:
        conn = pool.getconn()
        if conn.closed:
            log.warning("[SEF_HELPER][METASTORE][RDBMS] Received closed connection from pool; replacing it.")
            pool.putconn(conn, close=True)
            conn = pool.getconn()

        try:
            conn.autocommit = True
        except Exception:
            pass

        log.debug("[SEF_HELPER][METASTORE][RDBMS] Acquired connection from metastore pool.")
        return conn
    except Exception as exc:
        if "connection pool exhausted" in str(exc).lower():
            new_max = pool.maxconn + 5
            log.warning(
                f"[SEF_HELPER][METASTORE][RDBMS] Metastore connection pool exhausted. Expanding pool to {new_max}."
            )
            try:
                pool.closeall()
            except Exception:
                pass
            init_metastore_pool(pool.minconn, new_max)
            pool = METASTORE_POOL
            conn = pool.getconn()
            try:
                conn.autocommit = True
            except Exception:
                pass
            return conn
        log.error(f"[SEF_HELPER][METASTORE][RDBMS] Error getting metastore connection: {exc}")
        raise


def release_metastore_connection(conn) -> None:
    pool = init_metastore_pool()
    try:
        if conn.closed:
            pool.putconn(conn, close=True)
            log.debug("[SEF_HELPER][METASTORE][RDBMS] Closed dead metastore connection from pool.")
            return

        try:
            if not getattr(conn, "autocommit", True):
                conn.rollback()
        except Exception:
            pass

        pool.putconn(conn)
        log.debug("[SEF_HELPER][METASTORE][RDBMS] Released metastore connection back to pool.")
    except Exception as exc:
        log.error(f"[SEF_HELPER][METASTORE][RDBMS] Error releasing metastore connection: {exc}")
