from __future__ import annotations

import typing
if typing.TYPE_CHECKING:
    import psycopg

from toolsql import spec

from . import abstract_driver


class ConnectorxConnection(str):
    def __enter__(self) -> ConnectorxConnection:
        return self

    def __exit__(self, *args: typing.Any) -> None:
        pass

    def cursor(
        self, sql: str | None = None, parameters: typing.Any = None
    ) -> psycopg.Cursor[typing.Any]:
        raise NotImplementedError('no cursor() for connectorx')

    def execute(self, sql: str, parameters: typing.Any = None) -> None:
        raise NotImplementedError('no execute() for connectorx')

    def executemany(self, sql: str, parameters: typing.Any = None) -> None:
        raise NotImplementedError('no executemany() for connectorx')

    def close(self) -> None:
        pass


class ConnectorxAsyncConnection(str):
    async def __aenter__(self) -> ConnectorxAsyncConnection:
        return self

    async def __aexit__(self, *args: typing.Any) -> None:
        pass

    def __await__(
        self,
    ) -> typing.Generator[typing.Any, None, ConnectorxAsyncConnection]:
        async def closure() -> ConnectorxAsyncConnection:
            return self

        return closure().__await__()

    async def cursor(
        self, sql: str | None = None, parameters: typing.Any = None
    ) -> psycopg.Cursor[typing.Any]:
        raise NotImplementedError('no cursor() for connectorx')

    async def execute(self, sql: str, parameters: typing.Any = None) -> None:
        raise NotImplementedError('no execute() for connectorx')

    async def executemany(
        self, sql: str, parameters: typing.Any = None
    ) -> None:
        raise NotImplementedError('no executemany() for connectorx')

    async def close(self) -> None:
        pass


class ConnectorxDriver(abstract_driver.AbstractDriver):
    name = 'connectorx'

    @classmethod
    def connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.Connection:
        return ConnectorxConnection(uri)

    @classmethod
    def async_connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.AsyncConnection:
        return ConnectorxAsyncConnection(uri)

    @classmethod
    def executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams | None,
        conn: spec.Connection,
    ) -> None:
        raise Exception('connectorx cannot use execute() or executemany()')

    @classmethod
    async def async_executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams | None,
        conn: spec.AsyncConnection,
    ) -> None:
        raise Exception('connectorx cannot use execute() or executemany()')

