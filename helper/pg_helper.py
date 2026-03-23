import os

import psycopg2
from logger import log
from psycopg2 import sql
from psycopg2.pool import SimpleConnectionPool

from config.config import METASTORE_DB_HOST, METASTORE_DB, METASTORE_DB_PORT, METASTORE_DB_USER, METASTORE_DB_PASSWORD

# Global connection pool for the metastore
METASTORE_POOL = None
METASTORE_POOL_MIN = int(os.getenv("METASTORE_POOL_MIN", 1))
METASTORE_POOL_MAX = int(os.getenv("METASTORE_POOL_MAX", 5))


def _ensure_metastore_db():
    """Ensure that the metastore database exists."""
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
    """Initialize the connection pool for the metastore."""
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
            log.info(f"[SEF_HELPER][METASTORE][RDBMS] Metastore connection pool created (min={minconn}, max={maxconn}).")
        except Exception as e:
            log.error(f"[SEF_HELPER][METASTORE][RDBMS] Error establishing metastore connection pool: {e}")
            raise
    else:
        log.debug("[SEF_HELPER][METASTORE][RDBMS] Reusing existing metastore connection pool.")
    return METASTORE_POOL


def get_metastore_connection():
    """Get a connection from the metastore pool."""
    pool = init_metastore_pool()
    try:
        conn = pool.getconn()
        if conn.closed:
            log.warning("[SEF_HELPER][METASTORE][RDBMS] Received closed connection from pool; replacing it.")
            pool.putconn(conn, close=True)
            conn = pool.getconn()

        # psycopg2 defaults to autocommit=False, which caused metastore writes to stay uncommitted.
        try:
            conn.autocommit = True
        except Exception:
            pass

        log.debug("[SEF_HELPER][METASTORE][RDBMS] Acquired connection from metastore pool.")
        return conn
    except Exception as e:
        msg = str(e)
        if "connection pool exhausted" in msg.lower():
            new_max = pool.maxconn + 5
            log.warning(f"[SEF_HELPER][METASTORE][RDBMS] Metastore connection pool exhausted. Expanding pool to {new_max}.")
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
        log.error(f"[SEF_HELPER][METASTORE][RDBMS] Error getting metastore connection: {e}")
        raise


def release_metastore_connection(conn):
    """Return a connection back to the metastore pool."""
    pool = init_metastore_pool()
    try:
        if conn.closed:
            pool.putconn(conn, close=True)
            log.debug("[SEF_HELPER][METASTORE][RDBMS] Closed dead metastore connection from pool.")
        else:
            # If anyone ever turned autocommit off and left a transaction open, reset it.
            try:
                if not getattr(conn, "autocommit", True):
                    conn.rollback()
            except Exception:
                pass
            pool.putconn(conn)
            log.debug("[SEF_HELPER][METASTORE][RDBMS] Released metastore connection back to pool.")
    except Exception as e:
        log.error(f"[SEF_HELPER][METASTORE][RDBMS] Error releasing metastore connection: {e}")
