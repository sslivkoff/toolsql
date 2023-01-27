from __future__ import annotations

import typing

from toolsql import spec

from . import abstract_driver


class ConnectorxConn(str):
    def __enter__(self) -> ConnectorxConn:
        return self

    def __exit__(self, *args: typing.Any) -> None:
        pass

    def close(self) -> None:
        pass


class ConnectorxAsyncConn(str):
    async def __aenter__(self) -> ConnectorxAsyncConn:
        return self

    async def __aexit__(self, *args: typing.Any) -> None:
        pass

    def __await__(
        self,
    ) -> typing.Generator[typing.Any, None, ConnectorxAsyncConn]:
        async def closure() -> ConnectorxAsyncConn:
            return self

        return closure().__await__()

    async def close(self) -> None:
        pass


# class ConnectorxDriver(abstract_driver.AbstractDriver):
#     name = 'connectorx'

#     @classmethod
#     def connect(
#         cls,
#         uri: str,
#         *,
#         as_context: bool,
#         autocommit: bool,
#     ) -> spec.Connection:
#         return ConnectorxConn(uri)

#     @classmethod
#     def async_connect(
#         cls,
#         uri: str,
#         *,
#         as_context: bool,
#         autocommit: bool,
#     ) -> spec.Connection:
#         return ConnectorxAsyncConn(uri)

    # @classmethod
    # def executemany(
    #     cls,
    #     *,
    #     sql: str,
    #     parameters: spec.ExecuteManyParams | None,
    #     conn: spec.Connection,
    # ) -> None:
    #     raise Exception('connectorx cannot use execute() or executemany()')

    # @classmethod
    # async def async_executemany(
    #     cls,
    #     *,
    #     sql: str,
    #     parameters: spec.ExecuteManyParams,
    #     conn: spec.AsyncConnection,
    # ) -> None:
    #     raise Exception('connectorx cannot use execute() or executemany()')

