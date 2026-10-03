import os
from contextlib import asynccontextmanager
from typing import Any, Iterator, Optional

from fastapi import FastAPI
from psycopg2.pool import SimpleConnectionPool

TIMESCALEDB_DEV_DBNAME = "dashboardeelmobiliteit-timescaledb-dev"

pgpool: Optional[SimpleConnectionPool] = None
timescaledb_pgpool: Optional[SimpleConnectionPool] = None


def build_main_conn_str() -> str:
    conn_str = "dbname={}".format(os.getenv("DB_NAME"))
    for env_key, option in (
        ("DB_HOST", "host"),
        ("DB_USER", "user"),
        ("DB_PASSWORD", "password"),
        ("DB_PORT", "port"),
    ):
        if os.getenv(env_key):
            conn_str += " {}={}".format(option, os.environ[env_key])
    return conn_str


def build_timescaledb_conn_str() -> str:
    dbname = TIMESCALEDB_DEV_DBNAME if os.getenv("DEV") == "true" else os.getenv("TIMESCALEDB_NAME")
    conn_str = "dbname={}".format(dbname)
    for env_key, option in (
        ("TIMESCALE_DB_HOST", "host"),
        ("TIMESCALE_DB_USER", "user"),
        ("TIMESCALE_DB_PASSWORD", "password"),
        ("TIMESCALE_DB_PORT", "port"),
    ):
        if os.getenv(env_key):
            conn_str += " {}={}".format(option, os.environ[env_key])
    return conn_str


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pgpool, timescaledb_pgpool
    pgpool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=build_main_conn_str())
    timescaledb_pgpool = SimpleConnectionPool(minconn=1, maxconn=10, dsn=build_timescaledb_conn_str())
    yield
    if pgpool is not None:
        pgpool.closeall()
    if timescaledb_pgpool is not None:
        timescaledb_pgpool.closeall()


def get_conn() -> Iterator[Any]:
    if pgpool is None:
        raise RuntimeError("Main database pool is not initialized")
    conn = pgpool.getconn()
    try:
        yield conn
    finally:
        pgpool.putconn(conn)


def get_timescaledb_conn() -> Iterator[Any]:
    if timescaledb_pgpool is None:
        raise RuntimeError("TimescaleDB pool is not initialized")
    conn = timescaledb_pgpool.getconn()
    try:
        yield conn
    finally:
        timescaledb_pgpool.putconn(conn)
