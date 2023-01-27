from __future__ import annotations

import types
from toolsql import spec


class AbstractDriver:
    name: str

    def __init__(self) -> None:
        name = self.__class__.__name__
        raise Exception('do not instantiate ' + name + ', just use its methods')

    @classmethod
    def get_module(cls) -> types.ModuleType:
        import importlib

        return importlib.import_module(cls.name)

    #
    # # Connections
    #

    @classmethod
    def connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.Connection:
        raise NotImplementedError('connect() for ' + cls.name)

    @classmethod
    def async_connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.AsyncConnection:
        raise NotImplementedError('async_connect() for ' + cls.name)

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor | spec.AsyncCursor,
    ) -> tuple[str, ...] | None:
        raise NotImplementedError('get_cursor_output_names() for ' + cls.name)

    @classmethod
    def executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams | None,
        conn: spec.Connection,
    ) -> None:

        with conn.cursor() as cursor:  # type: ignore
            if parameters is None:
                cursor.execute(sql)
            else:
                cursor.executemany(sql, parameters)

    @classmethod
    async def async_executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams | None,
        conn: spec.AsyncConnection,
    ) -> None:
        async with conn.cursor() as cursor:
            await cursor.executemany(sql, parameters)

