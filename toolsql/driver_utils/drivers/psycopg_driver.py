from __future__ import annotations

import typing

import psycopg

from toolsql import conn_utils
from toolsql import spec
from . import abstract_driver


class PsycopgAsyncConnWrapper:
    """this wrapper modifies the semantics of psycopg AsyncConnection's
    - can be used in async contexts without "async with await" syntax
    - has identical semantics to aiosqlite conn
    """

    conn: typing.Awaitable[psycopg.AsyncConnection[typing.Any]]
    awaited: psycopg.AsyncConnection[typing.Any]

    def __init__(
        self, conn: typing.Awaitable[psycopg.AsyncConnection[typing.Any]]
    ) -> None:
        self.conn = conn

    async def __aenter__(self) -> psycopg.AsyncConnection[typing.Any]:
        self.awaited = await self.conn
        return await self.awaited.__aenter__()

    async def __aexit__(self, *args: typing.Any) -> None:
        return await self.awaited.__aexit__(*args)

    def __await__(
        self,
    ) -> typing.Generator[
        typing.Any, None, psycopg.AsyncConnection[typing.Any]
    ]:
        async def closure() -> psycopg.AsyncConnection[typing.Any]:
            if not hasattr(self, 'awaited'):
                self.awaited = await self.conn
            return self.awaited

        return closure().__await__()


class PsycopgDriver(abstract_driver.AbstractDriver):
    name = 'psycopg'

    @classmethod
    def get_psycopg_conn_str(cls, target: str | spec.DBConfig) -> str:
        if isinstance(target, str):
            db_config = conn_utils.parse_uri(target)
        elif isinstance(target, dict):
            db_config = target
        else:
            raise Exception('invalid connection target')
        return 'dbname={database} user={username}'.format(**db_config)

    @classmethod
    def connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.Connection:
        connect_str = cls.get_psycopg_conn_str(uri)
        return psycopg.connect(connect_str, autocommit=autocommit)

    @classmethod
    def get_cursor_output_names(
        cls,
        cursor: spec.Cursor | spec.AsyncCursor,
    ) -> tuple[str, ...] | None:
        description = cursor.description
        if description is None:
            return None
        else:
            return tuple(item.name for item in description)  # type: ignore

    @classmethod
    def async_connect(
        cls,
        uri: str,
        as_context: bool,
        autocommit: bool,
    ) -> spec.AsyncConnection:

        connect_str = cls.get_psycopg_conn_str(uri)
        if autocommit:
            conn = psycopg.AsyncConnection.connect(
                connect_str, autocommit=autocommit
            )
        else:
            conn = psycopg.AsyncConnection.connect(connect_str)

        return PsycopgAsyncConnWrapper(conn)  # type: ignore

