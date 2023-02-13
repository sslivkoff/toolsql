from __future__ import annotations

import typing

from toolsql import spec


class AbstractDriver:
    name: str

    def __init__(self) -> None:
        name = self.__class__.__name__
        raise Exception('do not instantiate ' + name + ', just use its methods')

    #
    # # connections
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

    #
    # # executions
    #

    @classmethod
    def execute(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.Connection,
    ) -> None:

        with conn.cursor() as cursor:  # type: ignore
            if parameters is None:
                cursor.execute(sql)
            else:
                cursor.execute(sql, parameters)

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
    async def async_execute(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.AsyncConnection,
    ) -> None:

        async with conn.cursor() as cursor:  # type: ignore
            if parameters is None:
                await cursor.execute(sql)
            else:
                await cursor.execute(sql, parameters)

    @classmethod
    async def async_executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams | None,
        conn: spec.AsyncConnection,
    ) -> None:
        async with conn.cursor() as cursor:  # type: ignore
            await cursor.executemany(sql, parameters)

    #
    # # select
    #

    @classmethod
    def _select(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.Connection | str | spec.DBConfig,
        decode_json_columns: typing.Sequence[int] | None = None,
        output_format: spec.QueryOutputFormat,
    ) -> spec.SelectOutput:
        raise NotImplementedError('_select() for ' + str(cls.__name__))

    @classmethod
    async def _async_select(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.AsyncConnection | str | spec.DBConfig,
        output_format: spec.QueryOutputFormat,
        decode_json_columns: typing.Sequence[int] | None = None,
    ) -> spec.AsyncSelectOutput:
        raise NotImplementedError('_async_select() for ' + str(cls.__name__))

