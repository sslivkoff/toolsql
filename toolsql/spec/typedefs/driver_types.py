from __future__ import annotations

import types
import typing
from typing_extensions import Literal
from typing_extensions import TypedDict

import aiosqlite
import psycopg
import sqlite3

from toolsql.drivers.driver_classes import abstract_driver
from toolsql.drivers.driver_classes import connectorx_driver


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


DBMS = Literal['sqlite', 'postgresql']


class DBConfig(TypedDict, total=False):
    dbms: DBMS
    driver: DriverName | None
    path: str | None
    hostname: str | None
    port: int | None
    database: str | None
    username: str | None
    password: str | None
    # socket: str
    # socket_dir: str
    # timeout: typing.Union[int, float]
    # pool_timeout: typing.Union[int, float]

