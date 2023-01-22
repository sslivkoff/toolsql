from __future__ import annotations

import functools
import sys
import types

from .. import spec
from .drivers.abstract_driver import AbstractDriver


def resolve_driver(driver: spec.DriverReference) -> spec.DriverClass:
    """convert a DriverReference reference into a DriverClass"""
    if isinstance(driver, type) and issubclass(driver, AbstractDriver):
        return driver

    if isinstance(driver, types.ModuleType):
        driver = driver.__name__

    if driver == 'sqlite3':
        from .drivers import sqlite3_driver

        return sqlite3_driver.Sqlite3Driver
    elif driver == 'aiosqlite':
        from .drivers import aiosqlite_driver

        return aiosqlite_driver.AiosqliteDriver
    elif driver == 'asyncpg':
        from .drivers import asyncpg_driver

        return asyncpg_driver.AsyncpgDriver
    elif driver == 'psycopg':
        from .drivers import psycopg_driver

        return psycopg_driver.PsycopgDriver
    elif driver == 'connectorx':
        from .drivers import connectorx_driver

        return connectorx_driver.ConnectorxDriver
    else:
        raise Exception('unknown driver: ' + str(driver))


def get_driver(
    *,
    sync: bool | None = None,
    output_format: spec.QueryOutputFormat | None = None,
    conn: spec.Connection | spec.AsyncConnection | None = None,
    db_config: spec.DBConfig | None = None,
    uri: str | None = None,
    driver: spec.DriverReference | None = None,
) -> spec.DriverClass:

    if driver is not None:
        return resolve_driver(driver=driver)

    elif db_config is not None and db_config.get('driver') is not None:
        return resolve_driver(db_config['driver'])

    elif conn is not None:
        return get_driver_of_conn(conn)

    elif output_format in ('polars', 'pandas'):
        return resolve_driver('connectorx')

    else:
        if sync is None:
            raise Exception('must specify more driver parameters')
        if db_config is not None:
            dbms = get_dbms(db_config)
        elif uri is not None:
            dbms = get_dbms(uri)
        else:
            raise Exception('must specify more parameters to determine driver')
        return get_driver_of_dbms(dbms=dbms, sync=sync)


def get_driver_of_conn(
    conn: spec.Connection | spec.AsyncConnection,
) -> spec.DriverClass:

    for driver_name in [
        'sqlite3',
        'aiosqlite',
        'psycopg',
        'asyncpg',
        'connectorx',
    ]:
        module = sys.modules.get(driver_name)
        if module is not None:
            if driver_name == 'connectorx':
                from .drivers import connectorx_driver

                if isinstance(
                    conn,
                    (
                        connectorx_driver.ConnectorxConn,
                        connectorx_driver.ConnectorxAsyncConn,
                    )
                ):
                    return resolve_driver('connectorx')
            else:
                ConnectionType = getattr(module, 'Connection', None)
                if isinstance(ConnectionType, type) and isinstance(
                    conn, ConnectionType
                ):
                    return resolve_driver(driver_name)
    else:
        raise Exception('could not determine driver of conn')


def get_driver_of_dbms(*, dbms: str, sync: bool) -> spec.DriverClass:
    if dbms == 'sqlite':
        if sync:
            return resolve_driver('sqlite3')
        else:
            return resolve_driver('aiosqlite')
    elif dbms == 'postgresql':
        if sync:
            return resolve_driver('psycopg')
        else:
            return resolve_driver('asyncpg')
    else:
        raise Exception('unknown dbms: ' + str(dbms))


def get_dbms(target: str | spec.DBConfig) -> str:

    if isinstance(target, dict):
        return target['dbms']
    elif target is not None:
        head, tail = target.split('://')
        if '+' in head:
            return head.split('+')[0]
        else:
            return head
    else:
        raise Exception('must specify uri, db_config, or conn')


@functools.lru_cache()
def parse_uri(uri: str) -> spec.DBConfig:
    if uri.startswith('sqlite://'):
        path = uri.split('sqlite://')[1]
        return {
            'dbms': 'sqlite',
            'path': path,
        }
    elif uri.startswith('postgres'):
        database = uri[uri.rfind('/') + 1 :]
        head, tail = uri.split('@')
        username = head.split('/')[-1]
        if ':' in username:
            username = username.split(':')[0]

        return {
            'dbms': 'postgresql',
            'database': database,
            'username': username,
        }
    else:
        raise Exception('unknown uri format')

