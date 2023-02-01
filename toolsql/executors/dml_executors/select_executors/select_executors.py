from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from toolsql import statements
from ... import ddl_executors
from . import dbapi_selection
from . import connectorx_selection


if typing.TYPE_CHECKING:
    from typing_extensions import Literal
    from typing_extensions import TypedDict
    from typing_extensions import Unpack

    class SelectKwargs(TypedDict, total=False):
        conn: spec.Connection | str | spec.DBConfig
        table: str | spec.TableSchema
        columns: typing.Sequence[str] | None
        where_equals: typing.Mapping[str, typing.Any] | None
        where_gt: typing.Mapping[str, typing.Any] | None
        where_gte: typing.Mapping[str, typing.Any] | None
        where_lt: typing.Mapping[str, typing.Any] | None
        where_lte: typing.Mapping[str, typing.Any] | None
        where_like: typing.Mapping[str, str] | None
        where_ilike: typing.Mapping[str, str] | None
        where_in: typing.Mapping[str, typing.Sequence[str]] | None
        order_by: spec.OrderBy | None
        limit: int | str | None
        offset: int | str | None
        cast: typing.Mapping[str, str] | None

    class RawSelectKwargs(TypedDict, total=False):
        conn: spec.Connection | str | spec.DBConfig
        sql: str
        parameters: spec.ExecuteParams | None
        raw_column_types: typing.Mapping[str, str] | None


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
    table: str | spec.TableSchema,
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
    cast: typing.Mapping[str, str] | None = None,
) -> spec.SelectOutput:

    # conn = _resolve_select_conn_reference(conn)

    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_select_statement(
        dialect=dialect,
        table=table,
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
        cast=cast,
    )

    # gather raw column types for sqlite JSON or connectorx json
    if _need_raw_column_types(dialect=dialect, conn=conn):
        raw_column_types = ddl_executors.get_table_raw_column_types(
            table=table, conn=conn
        )
    else:
        raw_column_types = None

    return raw_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
        raw_column_types=raw_column_types,
    )


def _need_raw_column_types(
    dialect: spec.Dialect, conn: spec.Connection | str | spec.DBConfig
) -> bool:

    if dialect == 'sqlite':
        return True

    driver = drivers.get_driver_class(conn=conn)
    if dialect == 'postgresql' and driver.name == 'connectorx':
        return True

    return False


@typing.overload
def raw_select(
    *, output_format: Literal['dict'], **kwargs: Unpack[RawSelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
def raw_select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[RawSelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
def raw_select(
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
def raw_select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.SelectOutput:
    ...


def raw_select(  # type: ignore
    sql: str,
    *,
    parameters: spec.ExecuteParams | None = None,
    conn: spec.Connection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    raw_column_types: typing.Mapping[str, str] | None = None,
) -> spec.SelectOutput:

    driver = drivers.get_driver_name(conn=conn)
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
            raw_column_types=raw_column_types,
        )


async def async_select(
    *,
    conn: spec.AsyncConnection | str,
    output_format: spec.QueryOutputFormat = 'dict',
    #
    # query parameters
    table: str | spec.TableSchema,
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

    dialect = drivers.get_conn_dialect(conn)
    sql, parameters = statements.build_select_statement(
        dialect=dialect,
        table=table,
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
    parameters: spec.ExecuteParams | None = None,
    conn: spec.AsyncConnection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    raw_column_types: typing.Mapping[str, str] | None = None,
) -> spec.AsyncSelectOutput:

    driver = drivers.get_driver_name(conn=conn)
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
            raw_column_types=raw_column_types,
        )

