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

SqlParameters = typing.Union[tuple[typing.Any, ...], dict[str, typing.Any]]

DictRows = typing.Sequence[typing.Dict[str, typing.Any]]
TupleRows = typing.Sequence[typing.Tuple[typing.Any, ...]]
RowOutput = typing.Union[
    DictRows,
    TupleRows,
    pl.DataFrame,
    pd.DataFrame,
]
SelectOutput = typing.Union[
    Cursor,
    DictRows,
    TupleRows,
    pl.DataFrame,
    pd.DataFrame,
]
AsyncSelectOutput = typing.Union[
    AsyncCursor,
    DictRows,
    TupleRows,
    pl.DataFrame,
    pd.DataFrame,
]


class OrderByDict(TypedDict):
    column: str
    asc: NotRequired[bool]
    desc: NotRequired[bool]


OrderByItem = typing.Union[str, OrderByDict]
OrderBy = typing.Union[OrderByItem, typing.Sequence[OrderByItem]]


ExecuteParams = tuple[typing.Any]
ExecuteManyParams = typing.Sequence[tuple[typing.Any]]

