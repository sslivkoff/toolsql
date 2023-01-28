from __future__ import annotations

import types
import typing
from typing_extensions import Literal
from typing_extensions import TypedDict

import aiosqlite
import psycopg
import sqlite3

from toolsql.driver_utils.drivers import abstract_driver
from toolsql.driver_utils.drivers import connectorx_driver


Dialect = Literal['sqlite', 'postgresql']
DriverName = Literal[
    'sqlite3',
    'aiosqlite',
    'psycopg',
    'connectorx',
]
DriverClass = type[abstract_driver.AbstractDriver]
DriverReference = typing.Union[
    str,
    types.ModuleType,
    DriverClass,
]

Connection = typing.Union[
    sqlite3.Connection,
    psycopg.Connection,
    connectorx_driver.ConnectorxConnection,
]
AsyncConnection = typing.Union[
    aiosqlite.Connection,
    psycopg.AsyncConnection,
    connectorx_driver.ConnectorxAsyncConnection,
]

FullConnection = typing.Union[
    sqlite3.Connection,
    psycopg.Connection,
    # connectorx_driver.ConnectorxConnection,
]
FullAsyncConnection = typing.Union[
    aiosqlite.Connection,
    psycopg.AsyncConnection,
    # connectorx_driver.ConnectorxAsyncConnection,
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

