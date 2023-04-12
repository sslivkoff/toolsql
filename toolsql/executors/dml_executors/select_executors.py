from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from toolsql import statements
from .. import ddl_executors


if typing.TYPE_CHECKING:
    from typing_extensions import Literal
    from typing_extensions import Unpack

    import polars as pl


@typing.overload
def select(
    *, output_format: Literal['dict'], **kwargs: Unpack[spec.SelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
def select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[spec.SelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
def select(
    *,
    output_format: Literal['single_column'],
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.TupleColumn:
    ...


@typing.overload
def select(
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
def select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[spec.SelectKwargs],
) -> spec.SelectOutput:
    ...


def select(  # type: ignore
    *,
    conn: spec.Connection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    #
    # query utils
    table: str | spec.TableSchema,
    columns: spec.ColumnsExpression | None = None,
    distinct: bool = False,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, typing.Any] | None = None,
    where_ilike: typing.Mapping[str, typing.Any] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
    order_by: spec.OrderBy | None = None,
    limit: int | str | None = None,
    offset: int | str | None = None,
    output_dtypes: spec.OutputDtypes | None = None,
    verbose: bool | int = False,
) -> spec.SelectOutput:

    if columns is not None and len(columns) == 0:
        raise Exception('empty selection in query')

    # gather raw column types for sqlite JSON or connectorx json
    dialect = drivers.get_conn_dialect(conn)
    columns, decode_columns, output_dtypes = _prepare_column_decoding(
        dialect=dialect,
        table=table,
        columns=columns,
        conn=conn,
        output_format=output_format,
        output_dtypes=output_dtypes,
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
    )

    if verbose:
        print(sql, parameters)

    return raw_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
        decode_columns=decode_columns,
        output_dtypes=output_dtypes,
    )


def _prepare_column_decoding(
    *,
    dialect: spec.Dialect,
    table: str | spec.TableSchema,
    conn: spec.Connection | str | spec.DBConfig,
    columns: spec.ColumnsExpression | None = None,
    output_format: spec.QueryOutputFormat = 'dict',
    output_dtypes: spec.OutputDtypes | None = None,
) -> tuple[
    spec.ColumnsExpression | None,
    spec.DecodeColumns | None,
    spec.OutputDtypes | None,
]:

    if isinstance(table, dict):
        raw_column_types: typing.Mapping[str, str] = {
            column['name']: column['type'] for column in table['columns']
        }
    else:
        raw_column_types = ddl_executors.get_table_raw_column_types(
            table=table, conn=conn
        )
    driver_name = drivers.get_driver_name(conn=conn)

    return _get_column_decode_types(
        dialect=dialect,
        raw_column_types=raw_column_types,
        driver_name=driver_name,
        columns=columns,
        output_format=output_format,
        output_dtypes=output_dtypes,
    )


def _get_column_decode_types(
    *,
    dialect: spec.Dialect,
    raw_column_types: typing.Mapping[str, str],
    driver_name: str,
    columns: spec.ColumnsExpression | None = None,
    output_format: spec.QueryOutputFormat = 'dict',
    output_dtypes: spec.OutputDtypes | None = None,
) -> tuple[
    spec.ColumnsExpression | None,
    spec.DecodeColumns | None,
    spec.OutputDtypes | None,
]:

    if output_format == 'polars' and output_dtypes is not None:
        new_output_dtypes: typing.MutableSequence[
            pl.datatypes.DataTypeClass | None
        ] | None = []
    else:
        new_output_dtypes = None

    columns = _normalize_columns(columns, raw_column_types=raw_column_types)

    # determine how to decode each column
    decode_columns: typing.MutableSequence[spec.DecodeColumn] = []
    if columns is None:
        for c, column_type in enumerate(raw_column_types.values()):
            if column_type in ('JSON', 'JSONB') and (
                dialect == 'sqlite'
                or (dialect == 'postgresql' and driver_name == 'connectorx')
            ):
                decode_columns.append('JSON')
            elif dialect == 'sqlite' and column_type == 'BOOLEAN':
                decode_columns.append('BOOLEAN')
            else:
                decode_columns.append(None)

            if new_output_dtypes is not None:
                new_output_type = spec.columntype_to_polars_dtype(
                    typing.cast(spec.Columntype, column_type)
                )
                new_output_dtypes.append(new_output_type)

    else:
        for column_expression in columns:
            column_type = raw_column_types.get(column_expression.get('column'))  # type: ignore
            if column_type in ('JSON', 'JSONB') and (
                dialect == 'sqlite'
                or (dialect == 'postgresql' and driver_name == 'connectorx')
            ):
                decode_columns.append('JSON')
            elif dialect == 'sqlite' and column_type == 'BOOLEAN':
                decode_columns.append('BOOLEAN')
            elif (
                dialect == 'postgresql'
                and column_expression['column'] is not None
                and column_expression['column'].startswith('SUM(')
                and raw_column_types.get(column_expression['column'][4:-1])
                in spec.integer_columntypes
            ):
                decode_columns.append('INTEGER')
            else:
                decode_columns.append(None)

            if new_output_dtypes is not None:
                new_output_type = spec.columntype_to_polars_dtype(
                    typing.cast(spec.Columntype, column_type)
                )
                new_output_dtypes.append(new_output_type)

    # cast columns as text
    if dialect == 'postgresql' and driver_name == 'connectorx':
        for c, decode_column in enumerate(decode_columns):
            if decode_column == 'JSON' and columns[c].get('cast') is None:
                columns[c]['cast'] = 'TEXT'

    if new_output_dtypes is not None:
        output_dtypes = new_output_dtypes

    return columns, decode_columns, output_dtypes


def _normalize_columns(
    columns: spec.ColumnsExpression | None,
    raw_column_types: typing.Mapping[str, str],
) -> typing.Sequence[spec.ColumnExpressionDict]:

    if columns is None:
        return [
            {'column': column_name} for column_name in raw_column_types.keys()
        ]

    normalized: list[spec.ColumnExpressionDict] = []
    if isinstance(columns, (list, tuple)):
        for column in columns:
            if isinstance(column, str):
                column_schema: spec.ColumnExpressionDict = {'column': column}
            elif isinstance(column, dict):
                column_schema = column  # type: ignore
            else:
                raise Exception('invalid column schema')
            normalized.append(column_schema)

    elif isinstance(columns, dict):
        for column_name, column_data in columns.items():
            if isinstance(column_data, str):
                column_schema = {'column': column_name, 'alias': column_data}
            elif isinstance(column_data, dict):
                column_schema = column_data.copy()  # type: ignore
                column_schema['column'] = column_name
            else:
                raise Exception('invalid column schema')
            normalized.append(column_schema)

    else:
        raise Exception('invalid columns specification')

    return normalized


@typing.overload
def raw_select(
    *, output_format: Literal['dict'], **kwargs: Unpack[spec.RawSelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
def raw_select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[spec.RawSelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
def raw_select(
    *,
    output_format: Literal['single_column'],
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.TupleColumn:
    ...


@typing.overload
def raw_select(
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
def raw_select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[spec.RawSelectKwargs],
) -> spec.SelectOutput:
    ...


def raw_select(  # type: ignore
    sql: str,
    *,
    parameters: spec.ExecuteParams | None = None,
    conn: spec.Connection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    decode_columns: spec.DecodeColumns | None = None,
    output_dtypes: spec.OutputDtypes | None = None,
) -> spec.SelectOutput:

    driver = drivers.get_driver_class(conn=conn)
    return driver._select(
        sql=sql,
        conn=conn,
        parameters=parameters,
        output_format=output_format,
        decode_columns=decode_columns,
        output_dtypes=output_dtypes,
    )


@typing.overload
async def async_select(
    *, output_format: Literal['dict'], **kwargs: Unpack[spec.AsyncSelectKwargs]
) -> spec.DictRows:
    ...


@typing.overload
async def async_select(
    *, output_format: Literal['tuple'], **kwargs: Unpack[spec.AsyncSelectKwargs]
) -> spec.TupleRows:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
async def async_select(
    *,
    output_format: Literal['single_column'],
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.TupleColumn:
    ...


@typing.overload
async def async_select(
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
async def async_select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[spec.AsyncSelectKwargs],
) -> spec.AsyncSelectOutput:
    ...


async def async_select(  # type: ignore
    *,
    conn: spec.AsyncConnection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    #
    # query parameters
    table: str | spec.TableSchema,
    columns: spec.ColumnsExpression | None = None,
    distinct: bool = False,
    where_equals: typing.Mapping[str, typing.Any] | None = None,
    where_gt: typing.Mapping[str, typing.Any] | None = None,
    where_gte: typing.Mapping[str, typing.Any] | None = None,
    where_lt: typing.Mapping[str, typing.Any] | None = None,
    where_lte: typing.Mapping[str, typing.Any] | None = None,
    where_like: typing.Mapping[str, typing.Any] | None = None,
    where_ilike: typing.Mapping[str, typing.Any] | None = None,
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None = None,
    where_or: typing.Sequence[spec.WhereGroup] | None = None,
    order_by: spec.OrderBy | None = None,
    limit: int | str | None = None,
    offset: int | str | None = None,
    output_dtypes: spec.OutputDtypes | None = None,
    verbose: bool | int = False,
) -> spec.AsyncSelectOutput:

    if columns is not None and len(columns) == 0:
        raise Exception('empty selection in query')

    dialect = drivers.get_conn_dialect(conn)
    (
        columns,
        decode_columns,
        output_dtypes,
    ) = await _async_prepare_column_decoding(
        dialect=dialect,
        table=table,
        columns=columns,
        conn=conn,
        output_format=output_format,
        output_dtypes=output_dtypes,
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
    )

    if verbose:
        print(sql, parameters)

    return await async_raw_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
        decode_columns=decode_columns,
        output_dtypes=output_dtypes,
    )


async def _async_prepare_column_decoding(
    *,
    dialect: spec.Dialect,
    table: str | spec.TableSchema,
    conn: spec.AsyncConnection | str | spec.DBConfig,
    columns: spec.ColumnsExpression | None = None,
    output_format: spec.QueryOutputFormat = 'dict',
    output_dtypes: spec.OutputDtypes | None = None,
) -> tuple[
    spec.ColumnsExpression | None,
    spec.DecodeColumns | None,
    spec.OutputDtypes | None,
]:

    if isinstance(table, dict):
        raw_column_types: typing.Mapping[str, str] = {
            column['name']: column['type'] for column in table['columns']
        }
    else:
        raw_column_types = await ddl_executors.async_get_table_raw_column_types(
            table=table, conn=conn
        )

    driver_name = drivers.get_driver_name(conn=conn)

    return _get_column_decode_types(
        dialect=dialect,
        raw_column_types=raw_column_types,
        driver_name=driver_name,
        columns=columns,
        output_format=output_format,
        output_dtypes=output_dtypes,
    )


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['dict'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['tuple'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.TupleRows:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_tuple'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.TupleRow:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_tuple_or_none'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.TupleRow | None:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_dict'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.DictRow:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_dict_or_none'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.DictRow | None:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['cell'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.Cell:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['cell_or_none'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.Cell | None:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: Literal['single_columns'],
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.TupleColumn:
    ...


@typing.overload
async def async_raw_select(
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.DictRows:
    ...


@typing.overload
async def async_raw_select(
    *,
    output_format: spec.QueryOutputFormat = 'dict',
    **kwargs: Unpack[spec.AsyncRawSelectKwargs],
) -> spec.AsyncSelectOutput:
    ...


async def async_raw_select(  # type: ignore
    sql: str,
    *,
    parameters: spec.ExecuteParams | None = None,
    conn: spec.AsyncConnection | str | spec.DBConfig,
    output_format: spec.QueryOutputFormat = 'dict',
    decode_columns: spec.DecodeColumns | None = None,
    output_dtypes: spec.OutputDtypes | None = None,
) -> spec.AsyncSelectOutput:

    driver = drivers.get_driver_class(conn=conn)
    return await driver._async_select(
        sql=sql,
        parameters=parameters,
        conn=conn,
        output_format=output_format,
        decode_columns=decode_columns,
        output_dtypes=output_dtypes,
    )

