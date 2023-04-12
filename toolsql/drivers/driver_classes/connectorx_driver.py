from __future__ import annotations

import os
import typing

if typing.TYPE_CHECKING:
    import psycopg

from toolsql import drivers
from toolsql import formats
from toolsql import spec
from toolsql import statements

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

    def execute(
        self, sql: str, parameters: typing.Any = None
    ) -> psycopg.Cursor[typing.Any]:
        raise NotImplementedError('no execute() for connectorx')

    def executemany(
        self, sql: str, parameters: typing.Any = None
    ) -> psycopg.Cursor[typing.Any]:
        raise NotImplementedError('no executemany() for connectorx')

    def fetchone(self) -> typing.Any:
        raise NotImplementedError('no fetchone() for connectorx')

    def fetchmany(self) -> typing.Any:
        raise NotImplementedError('no fetchmany() for connectorx')

    def fetchall(self) -> typing.Any:
        raise NotImplementedError('no fetchall() for connectorx')

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

    async def fetchone(self) -> typing.Any:
        raise NotImplementedError('no fetchone() for connectorx')

    async def fetchmany(self) -> typing.Any:
        raise NotImplementedError('no fetchmany() for connectorx')

    async def fetchall(self) -> typing.Any:
        raise NotImplementedError('no fetchall() for connectorx')

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
        timeout: int | None = None,
        extra_kwargs: typing.Any = None,
    ) -> spec.Connection:

        if uri.startswith('sqlite://'):
            path = uri.split('sqlite://')[1]
            if not os.path.isfile(path):
                raise spec.CannotConnect('path does not exist: ' + path)

        return ConnectorxConnection(uri)

    @classmethod
    def async_connect(
        cls,
        uri: str,
        *,
        as_context: bool,
        autocommit: bool,
        timeout: int | None = None,
        extra_kwargs: typing.Any = None,
    ) -> spec.AsyncConnection:

        if uri.startswith('sqlite://'):
            path = uri.split('sqlite://')[1]
            if not os.path.isfile(path):
                raise spec.CannotConnect('path does not exist: ' + path)

        return ConnectorxAsyncConnection(uri)

    @classmethod
    def executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams,
        conn: spec.Connection,
    ) -> None:
        raise Exception('connectorx cannot use execute() or executemany()')

    @classmethod
    async def async_executemany(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteManyParams,
        conn: spec.AsyncConnection,
    ) -> None:
        raise Exception('connectorx cannot use execute() or executemany()')

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
        output_format: spec.QueryOutputFormat,
        decode_columns: spec.DecodeColumns | None = None,
        output_dtypes: spec.OutputDtypes | None = None,
    ) -> spec.SelectOutput:

        import connectorx  # type: ignore

        if output_format == 'cursor':
            raise Exception('connectorx does not use cursors')
        elif output_format == 'pandas':
            result_format = 'pandas'
        else:
            result_format = 'polars'

        if parameters is not None and len(parameters) > 0:
            sql = statements.populate_sql_parameters(
                sql, parameters, dialect=drivers.get_conn_dialect(conn)
            )
            parameters = []

        if not isinstance(conn, str):
            if isinstance(conn, dict):
                conn = drivers.get_db_uri(conn)
            else:
                raise Exception('unknown conn format: ' + str(type(conn)))

        try:
            result = connectorx.read_sql(conn, sql, return_type=result_format)
        except Exception as e:
            raise spec.convert_exception(e)
        result = formats.decode_columns(rows=result, columns=decode_columns)
        return formats.format_row_dataframe(result, output_format=output_format)

    @classmethod
    async def _async_select(
        cls,
        *,
        sql: str,
        parameters: spec.ExecuteParams | None = None,
        conn: spec.AsyncConnection | str | spec.DBConfig,
        output_format: spec.QueryOutputFormat,
        decode_columns: spec.DecodeColumns | None = None,
        output_dtypes: spec.OutputDtypes | None = None,
    ) -> spec.AsyncSelectOutput:

        # see https://github.com/sfu-db/connector-x/discussions/368
        # see https://stackoverflow.com/a/69165563
        import asyncio

        if isinstance(conn, str) or isinstance(conn, dict):

            return await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: cls._select(
                    conn=conn,  # type: ignore
                    sql=sql,
                    parameters=parameters,
                    output_format=output_format,
                    decode_columns=decode_columns,
                    output_dtypes=output_dtypes,
                ),
            )
            # return await asyncio.to_thread(
            #     lambda: _select_connectorx(
            #         conn=conn,
            #         sql=sql,
            #         output_format=output_format,
            #     )
            # )

        else:
            raise Exception()

