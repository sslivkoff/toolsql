from __future__ import annotations

import typing

from toolsql import conn_utils
from toolsql import dialect_utils
from toolsql import driver_utils
from toolsql import spec
from . import dbapi_selection
from . import connectorx_selection


if typing.TYPE_CHECKING:
    from typing_extensions import Literal
    from typing_extensions import TypedDict
    from typing_extensions import Unpack

    class SelectKwargs(TypedDict, total=False):
        sql: str | None
        parameters: spec.SqlParameters | None
        conn: spec.Connection | str | spec.DBConfig


@typing.overload
def select(
    *, output_format: Literal['dict'], **kwargs: Unpack[SelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
def select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[SelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
def select(
    **kwargs: Unpack[SelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
def select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[SelectKwargs],
) -> spec.SelectOutput:
    ...


def select(  # type: ignore
    *,
    #
    # query parameters
    sql: str | None = None,
    parameters: spec.SqlParameters | None = None,
    #
    # execution parameters
    conn: spec.Connection | str | spec.DBConfig,
    #
    # output parameters
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.SelectOutput:

    # build query
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_select_query(
        sql=sql,
        parameters=parameters,
        dialect=dialect,
    )

    # execute query
    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'connectorx':
        if not isinstance(conn, (str, dict)):
            raise Exception('connectorx must use str or DBConfig for conn')
        return connectorx_selection._select_connectorx(
            sql=sql,
            conn=conn,
            output_format=output_format,
        )
    else:
        if isinstance(conn, (str, dict)):
            raise Exception('conn not initialized')
        return dbapi_selection._select_dbapi(
            sql=sql,
            parameters=parameters,
            conn=conn,  # type: ignore
            output_format=output_format,
            driver=driver,
        )


async def async_select(
    *,
    #
    # query parameters
    sql: str | None = None,
    parameters: spec.SqlParameters | None = None,
    #
    # connection parameters
    conn: spec.AsyncConnection | str,
    #
    # output parameters
    single_row: bool = False,
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.AsyncSelectOutput:

    # build query
    if sql is None:
        raise NotImplementedError('must specify sql')
    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_select_query(
        sql=sql,
        parameters=parameters,
        dialect=dialect,
    )

    # execute query
    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'connectorx':
        if not isinstance(conn, (str, dict)):
            raise Exception('connectorx must use str or DBConfig for conn')
        return await connectorx_selection._async_select_connectorx(
            sql=sql,
            conn=conn,
            output_format=output_format,
        )
    else:
        if isinstance(conn, str):
            raise Exception('conn not initialized')
        return await dbapi_selection._async_select_dbapi(
            sql=sql,
            parameters=parameters,
            conn=conn,
            output_format=output_format,
            driver=driver,
        )

