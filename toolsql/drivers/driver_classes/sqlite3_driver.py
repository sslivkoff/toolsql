from __future__ import annotations

import typing

import sqlite3

from toolsql import spec
from . import dbapi_driver


class Sqlite3Driver(dbapi_driver.DbapiDriver):
    name = 'sqlite3'

    @classmethod
    def connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
        timeout: int | None = None,
        extra_kwargs: typing.Any = None,
    ) -> spec.Connection:

        if extra_kwargs is None:
            extra_kwargs = {}

        if timeout is None:
            timeout = 30

        path = uri.split('sqlite://')[1]

        if autocommit:
            return sqlite3.connect(
                path, isolation_level=None, timeout=timeout, **extra_kwargs
            )
        else:
            return sqlite3.connect(path, timeout=timeout, **extra_kwargs)

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor | spec.AsyncCursor,
    ) -> tuple[str, ...] | None:
        if not isinstance(cursor, sqlite3.Cursor):
            raise Exception('not a sqlite3 cursor')
        return tuple(item[0] for item in cursor.description)

    @classmethod
    def executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams,
        conn: spec.Connection,
    ) -> None:

        if not isinstance(conn, sqlite3.dbapi2.Connection):
            raise Exception('not a sqlite conn')

        cursor = conn.cursor()
        try:
            try:
                cursor.executemany(sql, parameters)
            except Exception as e:
                raise spec.convert_exception(e, sql)
        finally:
            cursor.close()

    @classmethod
    def execute(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.Connection,
    ) -> None:

        if not isinstance(conn, sqlite3.dbapi2.Connection):
            raise Exception('not a sqlite conn')

        cursor = conn.cursor()
        try:
            try:
                if parameters is None:
                    cursor.execute(sql)
                else:
                    cursor.execute(sql, parameters)
            except Exception as e:
                raise spec.convert_exception(e, sql)
        finally:
            cursor.close()

