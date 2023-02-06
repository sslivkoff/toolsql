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
        distinct: bool
        where_equals: typing.Mapping[str, typing.Any] | None
        where_gt: typing.Mapping[str, typing.Any] | None
        where_gte: typing.Mapping[str, typing.Any] | None
        where_lt: typing.Mapping[str, typing.Any] | None
        where_lte: typing.Mapping[str, typing.Any] | None
        where_like: typing.Mapping[str, str] | None
        where_ilike: typing.Mapping[str, str] | None
        where_in: typing.Mapping[str, typing.Sequence[str]] | None
        where_or: typing.Sequence[spec.WhereGroup] | None
        order_by: spec.OrderBy | None
        limit: int | str | None
        offset: int | str | None
        cast: typing.Mapping[str, str] | None

    class RawSelectKwargs(TypedDict, total=False):
        conn: spec.Connection | str | spec.DBConfig
        sql: str
        parameters: spec.ExecuteParams | None
        raw_column_types: typing.Mapping[str, str] | None

    class AsyncSelectKwargs(TypedDict, total=False):
        conn: spec.AsyncConnection | str | spec.DBConfig
        table: str | spec.TableSchema
        columns: typing.Sequence[str] | None
        distinct: bool
        where_equals: typing.Mapping[str, typing.Any] | None
        where_gt: typing.Mapping[str, typing.Any] | None
        where_gte: typing.Mapping[str, typing.Any] | None
        where_lt: typing.Mapping[str, typing.Any] | None
        where_lte: typing.Mapping[str, typing.Any] | None
        where_like: typing.Mapping[str, str] | None
        where_ilike: typing.Mapping[str, str] | None
        where_in: typing.Mapping[str, typing.Sequence[str]] | None
        where_or: typing.Sequence[spec.WhereGroup] | None
        order_by: spec.OrderBy | None
        limit: int | str | None
        offset: int | str | None
        cast: typing.Mapping[str, str] | None

    class AsyncRawSelectKwargs(TypedDict, total=False):
        conn: spec.AsyncConnection | str | spec.DBConfig
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
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[SelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[SelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[SelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[SelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[SelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[SelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_column'],
    **kwargs: Unpack[SelectKwargs],
) -> spec.TupleColumn:
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
    distinct: bool = False,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
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
        distinct=distinct,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        where_or=where_or,
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
    *,
    dialect: spec.Dialect,
    table: str | spec.TableSchema,
    conn: spec.Connection | str | spec.DBConfig,
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

        if columns is None:
            raw_column_types = ddl_executors.get_table_raw_column_types(
                table=table, conn=conn
            )
            if drivers.get_driver_name(conn=conn) == 'connectorx':
                if cast is not None:
                    cast = dict(cast)
                else:
                    cast = {}
                for column_name, column_type in raw_column_types.items():
                    if column_type == 'JSONB':
                        if column_name in cast:
                            raise Exception(
                                'cannot cast JSON column: ' + str(column_name)
                            )
                        cast[column_name] = 'TEXT'
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
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_column'],
    **kwargs: Unpack[RawSelectKwargs],
) -> spec.TupleColumn:
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


@typing.overload
async def async_select(
    *, output_format: Literal['dict'], **kwargs: Unpack[AsyncSelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
async def async_select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[AsyncSelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_column'],
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.TupleColumn:
    ...


@typing.overload
async def async_select(
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
async def async_select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[AsyncSelectKwargs],
) -> spec.AsyncSelectOutput:
    ...


async def async_select(  # type: ignore
    *,
    conn: spec.AsyncConnection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    #
    # query parameters
    table: str | spec.TableSchema,
    columns: typing.Sequence[str] | None = None,
    distinct: bool = False,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, str] | None = None,
    where_ilike: typing.Mapping[str, str] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[str]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
    order_by: spec.OrderBy | None = None,
    limit: int | str | None = None,
    offset: int | str | None = None,
    cast: typing.Mapping[str, str] | None = None,
) -> spec.AsyncSelectOutput:

    dialect = drivers.get_conn_dialect(conn)
    columns, raw_column_types, cast = await _async_handle_json_columns(
        dialect=dialect,
        table=table,
        columns=columns,
        conn=conn,
        cast=cast,
    )
    sql, parameters = statements.build_select_statement(
        dialect=dialect,
        table=table,
        columns=columns,
        distinct=distinct,
        where_equals=where_equals,
        where_gt=where_gt,
        where_gte=where_gte,
        where_lt=where_lt,
        where_lte=where_lte,
        where_like=where_like,
        where_ilike=where_ilike,
        where_in=where_in,
        where_or=where_or,
        order_by=order_by,
        limit=limit,
        offset=offset,
        cast=cast,
    )

    return await async_raw_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
        raw_column_types=raw_column_types,
    )


async def _async_handle_json_columns(
    *,
    dialect: spec.Dialect,
    table: str | spec.TableSchema,
    conn: spec.AsyncConnection | str | spec.DBConfig,
    cast: typing.Mapping[str, str] | None = None,
    columns: typing.Sequence[str] | None = None,
) -> tuple[
    typing.Sequence[str] | None,
    typing.Mapping[str, str] | None,
    typing.Mapping[str, str] | None,
]:

    # for sqlite, gather raw column types, used for decoding later
    if dialect == 'sqlite':
        raw_column_types = await ddl_executors.async_get_table_raw_column_types(
            table=table, conn=conn
        )
        return columns, raw_column_types, cast

    # for postgres x connectorx, cast columns as text, used for decoding later
    driver = drivers.get_driver_class(conn=conn)
    if dialect == 'postgresql' and driver.name == 'connectorx':

        if columns is None:
            raw_column_types = await ddl_executors.async_get_table_raw_column_types(
                table=table, conn=conn
            )
            if drivers.get_driver_name(conn=conn) == 'connectorx':
                if cast is not None:
                    cast = dict(cast)
                else:
                    cast = {}
                for column_name, column_type in raw_column_types.items():
                    if column_type == 'JSONB':
                        if column_name in cast:
                            raise Exception(
                                'cannot cast JSON column: ' + str(column_name)
                            )
                        cast[column_name] = 'TEXT'
            columns = list(raw_column_types.keys())
            return columns, raw_column_types, cast

    return columns, None, cast


@typing.overload
async def async_raw_select(
    *, output_format: Literal['dict'], **kwargs: Unpack[AsyncRawSelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
async def async_raw_select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[AsyncRawSelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_columns'],
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.TupleColumn:
    ...


@typing.overload
async def async_raw_select(
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[AsyncRawSelectKwargs],
) -> spec.AsyncSelectOutput:
    ...


async def async_raw_select(  # type: ignore
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

