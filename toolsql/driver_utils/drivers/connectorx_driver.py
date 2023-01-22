from __future__ import annotations

import typing

from toolsql import spec
import connectorx  # type: ignore

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
        return ConnectorxConn(uri)

    @classmethod
    def async_connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
    ) -> spec.Connection:
        return ConnectorxAsyncConn(uri)

    @classmethod
    def select(
        cls,
        conn: spec.Connection,
        sql: str,
        parameters: spec.SqlParameters,
        output_format: spec.QueryOutputFormat | None = None,
    ) -> spec.SelectOutput:

        # compile sql
        compiled_sql = cls.compile_sql(sql, parameters)

        # execute
        if isinstance(conn, str):
            conn_str = conn
        else:
            raise Exception('connectorx conn must be str')
        if output_format == 'pandas':
            result_format = 'pandas'
        else:
            result_format = 'polars'
        result = connectorx.read_sql(
            conn_str, compiled_sql, return_type=result_format
        )

        # convert to output_format
        if output_format is None:
            output_format = 'polars'
        if output_format == 'polars':
            import polars as pl

            if isinstance(result, pl.DataFrame):
                return result
            else:
                raise Exception('improper format')
        elif output_format == 'pandas':
            import pandas as pd

            if isinstance(result, pd.DataFrame):
                return result
            else:
                raise Exception('improper format')
        elif output_format == 'tuple':
            return list(zip(*result.to_dict().values()))
        elif output_format == 'dict':
            as_dicts: spec.DictRows = result.to_dicts()
            return as_dicts
        else:
            raise Exception('unknown output format')

    @classmethod
    async def async_select(
        cls,
        conn: spec.AsyncConnection,
        sql: str,
        parameters: spec.SqlParameters,
        output_format: spec.QueryOutputFormat | None = None,
    ) -> spec.AsyncSelectOutput:

        # see https://github.com/sfu-db/connector-x/discussions/368
        # see https://stackoverflow.com/a/69165563
        import asyncio

        if isinstance(conn, str):
            conn_str: str = conn
        else:
            raise Exception('connectorx requires a str uri conn')

        return await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: cls.select(
                conn=conn_str,
                sql=sql,
                parameters=parameters,
                output_format=output_format,
            ),
        )

    @classmethod
    def compile_sql(cls, sql: str, parameters: spec.SqlParameters) -> str:
        if parameters is not None and len(parameters) > 0:
            raise NotImplementedError('compile_sql() for ' + str(cls.__name__))
        else:
            return sql

