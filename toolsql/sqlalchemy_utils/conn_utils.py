from __future__ import annotations

import typing

import sqlalchemy  # type: ignore

from .. import spec
from . import engine_utils


def create_connection(
    *,
    engine: typing.Optional[spec.SAEngine] = None,
    conn: typing.Optional[spec.SAConnection] = None,
    db_config: typing.Optional[spec.DBConfig] = None,
) -> spec.SAConnection:
    """create a sqlalchemy database conection"""
    if conn is None:
        if engine is None:
            if db_config is None:
                raise Exception('must specify conn, engine, or db_config')
            engine = engine_utils.create_engine(db_config=db_config)
        conn = engine.connect()

        # enable foreign keys for sqlite
        if (
            type(engine.dialect)
            == sqlalchemy.dialects.sqlite.pysqlite.SQLiteDialect_pysqlite
        ):
            conn.execute('PRAGMA foreign_keys=ON')

    return conn

