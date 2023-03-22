from __future__ import annotations

import typing

import aiosqlite

from toolsql import spec
from . import dbapi_driver


class AiosqliteDriver(dbapi_driver.DbapiDriver):
    name = 'aiosqlite'

    @classmethod
    def async_connect(
        cls,
        uri: str,
        as_context: bool,
        autocommit: bool,
        timeout: int | None = None,
        extra_kwargs: typing.Any = None,
    ) -> spec.AsyncConnection:

        if timeout is None:
            timeout = 600
        if extra_kwargs is None:
            extra_kwargs = {}

        path = uri.split('sqlite://')[1]

        if autocommit:
            return aiosqlite.connect(
                path, isolation_level=None, timeout=timeout, **extra_kwargs
            )
        else:
            return aiosqlite.connect(path, timeout=timeout, **extra_kwargs)

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor | spec.AsyncCursor,
    ) -> tuple[str, ...] | None:
        if not isinstance(cursor, aiosqlite.Cursor):
            raise Exception('not an aiosqlite cursor')
        return tuple(item[0] for item in cursor.description)

    @classmethod
    async def async_execute(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.AsyncConnection,
    ) -> None:

        if not isinstance(conn, aiosqlite.Connection):
            raise Exception('not an aiosqlite conn')

        try:
            if parameters is None:
                await conn.execute(sql)
            else:
                await conn.execute(sql, parameters)
        except Exception as e:
            raise spec.convert_exception(e, sql)

