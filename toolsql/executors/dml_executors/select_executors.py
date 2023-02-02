from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from toolsql import statements
from .. import ddl_executors


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

    # gather raw column types for sqlite JSON or connectorx json
    dialect = drivers.get_conn_dialect(conn)
    columns, raw_column_types, cast = _handle_json_columns(
        dialect=dialect,
        table=table,
        columns=columns,
        conn=conn,
        cast=cast,
    )

    # create query
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

    return raw_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
        raw_column_types=raw_column_types,
    )


def _handle_json_columns(
    dialect: spec.Dialect,
    table: str | spec.TableSchema,
    conn: spec.Connection,
    cast: typing.Mapping[str, str] | None = None,
    columns: typing.Sequence[str] | None = None,
) -> tuple[
    typing.Sequence[str] | None,
    typing.Mapping[str, str] | None,
    typing.Mapping[str, str] | None,
]:

    # for sqlite, gather raw column types, used for decoding later
    if dialect == 'sqlite':
        raw_column_types = ddl_executors.get_table_raw_column_types(
            table=table, conn=conn
        )
        return columns, raw_column_types, cast

    # for postgres x connectorx, cast columns as text, used for decoding later
    driver = drivers.get_driver_class(conn=conn)
    if dialect == 'postgresql' and driver.name == 'connectorx':
        raw_column_types = ddl_executors.get_table_raw_column_types(
            table=table, conn=conn
        )
        if drivers.get_driver_name(conn=conn) == 'connectorx':
            if cast is not None:
                cast = cast.copy()
            else:
                cast = {}
            for column_name, column_type in raw_column_types.items():
                if column_type == 'JSONB':
                    if column_name in cast:
                        raise Exception(
                            'cannot cast JSON column: ' + str(column_name)
                        )
                    cast[column_name] = 'TEXT'
        if columns is None:
            columns = list(raw_column_types.keys())
        return columns, raw_column_types, cast

    return columns, None, cast


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

    driver = drivers.get_driver_class(conn=conn)
    return driver._select(
        sql=sql,
        conn=conn,
        parameters=parameters,
        output_format=output_format,
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

    driver = drivers.get_driver_class(conn=conn)
    return await driver._async_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
        raw_column_types=raw_column_types,
    )

