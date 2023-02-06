from __future__ import annotations

import typing
from typing_extensions import Literal
from typing_extensions import TypedDict
from typing_extensions import NotRequired

import aiosqlite
import pandas as pd
import polars as pl
import psycopg
import sqlite3


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

#
# # output params
#

TupleRow = typing.Tuple[typing.Any, ...]
TupleRows = typing.Sequence[TupleRow]
DictRow = typing.Dict[str, typing.Any]
DictRows = typing.Sequence[DictRow]
Cell = typing.Any
TupleColumn = typing.Tuple[typing.Any, ...]
RowOutput = typing.Union[
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
    where_like: typing.Mapping[str, str] | None
    where_ilike: typing.Mapping[str, str] | None
    where_in: typing.Mapping[str, typing.Sequence[str]] | None
    where_or: typing.Sequence[WhereGroup] | None

