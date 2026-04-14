import os
import threading
from contextlib import contextmanager

import psycopg2
import psycopg2.pool

_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_pool_lock = threading.Lock()
_POOL_MIN = int(os.getenv("DB_POOL_MIN", "1"))
_POOL_MAX = int(os.getenv("DB_POOL_MAX", "5"))


def _db_url() -> str | None:
    return os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")


def _get_pool() -> psycopg2.pool.ThreadedConnectionPool | None:
    global _pool
    if _pool is not None:
        return _pool

    with _pool_lock:
        if _pool is not None:
            return _pool
        db_url = _db_url()
        if not db_url:
            return None
        _pool = psycopg2.pool.ThreadedConnectionPool(
            _POOL_MIN,
            _POOL_MAX,
            db_url,
            connect_timeout=5,
        )
    return _pool


@contextmanager
def _db_conn():
    pool = _get_pool()
    if pool is None and not _db_url():
        yield None
        return

    conn = None
    from_pool = False
    try:
        if pool is not None:
            conn = pool.getconn()
            from_pool = True

        if conn is None:
            conn = psycopg2.connect(_db_url(), connect_timeout=5)

        yield conn
    except Exception:
        yield None
    finally:
        if conn is None:
            return
        if from_pool and pool is not None:
            try:
                pool.putconn(conn)
                return
            except Exception:
                pass
        conn.close()
