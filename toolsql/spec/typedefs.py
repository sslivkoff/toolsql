from __future__ import annotations

import typing
import types
from typing_extensions import Literal
from typing_extensions import TypedDict

import aiosqlite
import asyncpg  # type: ignore
import pandas as pd
import polars as pl
import psycopg
import sqlite3

from toolsql.driver_utils.drivers.abstract_driver import AbstractDriver


DriverName = Literal[
    'sqlite3',
    'aiosqlite',
    'psycopg',
    'asyncpg',
    'connectorx',
]
DriverClass = type[AbstractDriver]
DriverReference = typing.Union[
    str,
    types.ModuleType,
    DriverClass,
]

Connection = typing.Union[
    sqlite3.Connection,
    psycopg.Connection,
    str,  # connectorx
]
AsyncConnection = typing.Union[
    aiosqlite.Connection,
    asyncpg.Connection,
    str,  # connectorx
]

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
    asyncpg.cursor.Cursor,
    psycopg.AsyncCursor,
    psycopg.AsyncServerCursor,
    psycopg.AsyncClientCursor,
]

SqlParameters = typing.Union[tuple[typing.Any, ...], dict[str, typing.Any]]

DictRows = typing.List[typing.Dict[str, typing.Any]]
TupleRows = typing.List[typing.Tuple[typing.Any, ...]]
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


DatabaseSystem = Literal['sqlite', 'postgresql']


class DBConfig(TypedDict, total=False):
    dbms: DatabaseSystem
    driver: DriverName
    path: str
    engine: str
    hostname: str
    port: int
    database: str
    username: str
    password: str
    socket: str
    socket_dir: str
    timeout: typing.Union[int, float]
    pool_timeout: typing.Union[int, float]

