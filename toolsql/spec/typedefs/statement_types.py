from __future__ import annotations

import typing
from typing_extensions import Literal
from typing_extensions import TypedDict
from typing_extensions import NotRequired

import aiosqlite
import pandas as pd  # type: ignore
import polars as pl
import psycopg
import sqlite3

from . import driver_types
from . import schema_types


QueryOutputFormat = Literal[
    'cursor',
    'dict',
    'tuple',
    'polars',
    'pandas',
    'single_tuple',  # reqiure output is single row, return row as tuple
    'single_tuple_or_none',  # like single_tuple, but None if no results
    'single_dict',  # require output is single row, return row as dict
    'single_dict_or_none',  # like single_dict, but None if no results
    'cell',  # require output is single row and single column, return it
    'cell_or_none',  # like cell, but None if no results
    'single_column',
]
Cursor = typing.Union[
    sqlite3.Cursor,
    psycopg.Cursor,
]
AsyncCursor = typing.Union[
    aiosqlite.Cursor,
    psycopg.AsyncCursor,
    psycopg.AsyncServerCursor,
    psycopg.AsyncClientCursor,
]

OutputDtypes = typing.Sequence[typing.Union[pl.datatypes.DataTypeClass, None]]

#
# # output params
#

TupleRow = typing.Tuple[typing.Any, ...]
TupleRows = typing.Sequence[TupleRow]
DictRow = typing.Dict[str, typing.Any]
DictRows = typing.Sequence[DictRow]
Cell = typing.Any
TupleColumn = typing.Tuple[typing.Any, ...]
SelectOutputData = typing.Union[
    TupleRow,
    TupleRows,
    DictRow,
    DictRows,
    Cell,
    TupleColumn,
    pl.DataFrame,
    pd.DataFrame,
    None,
]
SelectOutput = typing.Union[
    Cursor,
    TupleRow,
    TupleRows,
    DictRow,
    DictRows,
    Cell,
    TupleColumn,
    pl.DataFrame,
    pd.DataFrame,
    None,
]
AsyncSelectOutput = typing.Union[
    AsyncCursor,
    TupleRow,
    TupleRows,
    DictRow,
    DictRows,
    Cell,
    TupleColumn,
    pl.DataFrame,
    pd.DataFrame,
    None,
]


#
# # input params
#


class OrderByDict(TypedDict):
    column: str
    asc: NotRequired[bool]
    desc: NotRequired[bool]


OrderByItem = typing.Union[str, OrderByDict]
OrderBy = typing.Union[OrderByItem, typing.Sequence[OrderByItem]]

OnConflictOption = Literal['ignore', 'update']

ExecuteParams = typing.Union[
    typing.Sequence[typing.Any],
    typing.Mapping[str, typing.Any],
]
ExecuteManyParams = typing.Sequence[ExecuteParams]


class WhereGroup(TypedDict, total=False):
    where_equals: typing.Mapping[str, typing.Any] | None
    where_gt: typing.Mapping[str, typing.Any] | None
    where_gte: typing.Mapping[str, typing.Any] | None
    where_lt: typing.Mapping[str, typing.Any] | None
    where_lte: typing.Mapping[str, typing.Any] | None
    where_like: typing.Mapping[str, typing.Any] | None
    where_ilike: typing.Mapping[str, typing.Any] | None
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None
    where_or: typing.Sequence[WhereGroup] | None


class SelectKwargs(TypedDict, total=False):
    conn: driver_types.Connection | str | driver_types.DBConfig
    table: str | schema_types.TableSchema
    columns: typing.Sequence[str] | None
    distinct: bool
    where_equals: typing.Mapping[str, typing.Any] | None
    where_gt: typing.Mapping[str, typing.Any] | None
    where_gte: typing.Mapping[str, typing.Any] | None
    where_lt: typing.Mapping[str, typing.Any] | None
    where_lte: typing.Mapping[str, typing.Any] | None
    where_like: typing.Mapping[str, typing.Any] | None
    where_ilike: typing.Mapping[str, typing.Any] | None
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None
    where_or: typing.Sequence[WhereGroup] | None
    order_by: OrderBy | None
    limit: int | str | None
    offset: int | str | None
    output_dtypes: OutputDtypes | None


class RawSelectKwargs(TypedDict, total=False):
    conn: driver_types.Connection | str | driver_types.DBConfig
    sql: str
    parameters: ExecuteParams | None
    decode_columns: DecodeColumns | None
    output_dtypes: OutputDtypes | None


class AsyncSelectKwargs(TypedDict, total=False):
    conn: driver_types.AsyncConnection | str | driver_types.DBConfig
    table: str | schema_types.TableSchema
    columns: typing.Sequence[str] | None
    distinct: bool
    where_equals: typing.Mapping[str, typing.Any] | None
    where_gt: typing.Mapping[str, typing.Any] | None
    where_gte: typing.Mapping[str, typing.Any] | None
    where_lt: typing.Mapping[str, typing.Any] | None
    where_lte: typing.Mapping[str, typing.Any] | None
    where_like: typing.Mapping[str, typing.Any] | None
    where_ilike: typing.Mapping[str, typing.Any] | None
    where_in: typing.Mapping[str, typing.Sequence[typing.Any]] | None
    where_or: typing.Sequence[WhereGroup] | None
    order_by: OrderBy | None
    limit: int | str | None
    offset: int | str | None
    output_dtypes: OutputDtypes | None


class AsyncRawSelectKwargs(TypedDict, total=False):
    conn: driver_types.AsyncConnection | str | driver_types.DBConfig
    sql: str
    parameters: ExecuteParams | None
    decode_columns: DecodeColumns | None
    output_dtypes: OutputDtypes | None


#
# column expressions
#


class ColumnExpressionDict(TypedDict, total=False):
    column: str | None
    encode: Literal['raw_hex', 'prefix_hex'] | None
    # function: str | FunctionExpression
    cast: schema_types.Columntype | None
    alias: str | None


ColumnExpression = typing.Union[str, ColumnExpressionDict]

ColumnsExpression = typing.Sequence[ColumnExpression]

DecodeColumn = typing.Literal['JSON', 'BOOLEAN', 'INTEGER', None]
DecodeColumns = typing.Sequence[DecodeColumn]

