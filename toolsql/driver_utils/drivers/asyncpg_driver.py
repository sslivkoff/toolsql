from __future__ import annotations

import typing

import asyncpg  # type: ignore

from toolsql import spec
from . import abstract_driver


class AsyncpgConnContext:
    def __init__(self, uri: str) -> None:
        self.uri = uri

    async def __aenter__(self) -> asyncpg.Connection:
        return await asyncpg.connect(self.uri)

    async def __aexit__(self, *args: typing.Any) -> None:
        pass


class AsyncpgDriver(abstract_driver.AbstractDriver):
    name = 'asyncpg'

    @classmethod
    def get_cursor_output_names(cls, cursor: spec.Cursor) -> tuple[str] | None:
        if not isinstance(cursor, asyncpg.cursor.Cursor):
            raise Exception('not an asyncpg cursor')
        names: tuple[str] = cursor.get_attributes()
        return names

    @classmethod
    def async_connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.AsyncConnection:
        if not autocommit:
            raise Exception('asyncpg only supports autocommit=True')
        if as_context:
            return AsyncpgConnContext(uri)
        else:
            return asyncpg.connect(uri)

    @classmethod
    async def async_select(
        cls,
        conn: spec.AsyncConnection,
        sql: str,
        parameters: spec.SqlParameters,
        output_format: spec.QueryOutputFormat | None = None,
    ) -> spec.AsyncSelectOutput:

        # if not isinstance(conn, asyncpg.cursor.Cursor):
        if not isinstance(conn, asyncpg.Connection):
            raise Exception('not an asyncpg connection')

        if output_format == 'cursor':
            if not conn.is_in_transaction():
                raise Exception(
                    'asyncpg cannot use cursors inside transcations'
                )
            else:
                cursor: asyncpg.cursor.AsyncCursor = conn.cursor(
                    sql, *parameters
                )
                return cursor

        else:

            if len(parameters) == 0:
                rows = await conn.fetch(sql)
            else:
                rows = await conn.fetch(sql, parameters)
            if output_format == 'tuple':
                return [tuple(row) for row in rows]
            elif output_format == 'dict':
                return [dict(row) for row in rows]
            elif output_format == 'polars':
                import polars as pl

                if len(rows) > 0:
                    return pl.DataFrame(
                        [tuple(row) for row in rows],
                        columns=list(rows[0].keys()),
                    )
                else:
                    return pl.DataFrame(rows)
            elif output_format == 'pandas':
                import pandas as pd

                if len(rows) > 0:
                    return pd.DataFrame(
                        [tuple(row) for row in rows],
                        columns=list(rows[0].keys()),
                    )
                else:
                    return pd.DataFrame(rows)
            else:
                raise Exception('unknown output format: ' + str(output_format))

    # @classmethod
    # def transaction(cls, conn: spec.AsyncConnection) -> None:
    #     return conn.transaction()

