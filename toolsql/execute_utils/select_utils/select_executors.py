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
        conn: spec.Connection | str | spec.DBConfig
        sql: str
        parameters: spec.ExecuteParams | None


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
    conn: spec.Connection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    #
    # query utils
    table_name: str | None = None,
    columns: typing.Sequence[str] | None = None,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
    order_by: spec.OrderBy | None = None,
    limit: int | str | None = None,
    offset: int | str | None = None,
) -> spec.SelectOutput:

    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_select_query(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )

    return raw_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
    )


@typing.overload
def raw_select(
    *, output_format: Literal['dict'], **kwargs: Unpack[SelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
def raw_select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[SelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
def raw_select(
    **kwargs: Unpack[SelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
def raw_select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[SelectKwargs],
) -> spec.SelectOutput:
    ...


def raw_select(  # type: ignore
    sql: str,
    *,
    parameters: spec.ExecuteParams | None,
    conn: spec.Connection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.SelectOutput:

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
    conn: spec.AsyncConnection | str,
    output_format: spec.QueryOutputFormat = 'dict',
    #
    # query parameters
    table_name: str | None = None,
    columns: typing.Sequence[str] | None = None,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
    order_by: spec.OrderBy | None = None,
    limit: int | str | None = None,
    offset: int | str | None = None,
) -> spec.AsyncSelectOutput:

    dialect = conn_utils.get_conn_dialect(conn)
    sql, parameters = dialect_utils.build_select_query(
        dialect=dialect,
        table_name=table_name,
        columns=columns,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        order_by=order_by,
        limit=limit,
        offset=offset,
    )

    return await async_raw_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
    )


async def async_raw_select(
    sql: str,
    *,
    parameters: spec.ExecuteParams,
    conn: spec.AsyncConnection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
) -> spec.AsyncSelectOutput:

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
        if isinstance(conn, dict):
            raise Exception('conn not initialized')
        return await dbapi_selection._async_select_dbapi(
            sql=sql,
            parameters=parameters,
            conn=conn,
            output_format=output_format,
            driver=driver,
        )

