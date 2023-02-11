from __future__ import annotations

import sys

from toolsql import spec
from . import conn_utils
from .driver_classes import abstract_driver


def get_driver_name(
    driver: spec.DriverReference | None = None,
    *,
    sync: bool | None = None,
    output_format: spec.QueryOutputFormat | None = None,
    db_config: spec.DBConfig | None = None,
    uri: str | None = None,
    conn: spec.Connection
    | spec.AsyncConnection
    | str
    | spec.DBConfig
    | None = None,
) -> str:

    if (
        driver is not None
        and isinstance(driver, type)
        and issubclass(driver, abstract_driver.AbstractDriver)
    ):
        return driver.name

    elif isinstance(driver, str):
        if driver in ['sqlite3', 'aiosqlite', 'psycopg', 'connectorx']:
            return driver
        else:
            raise Exception('unknown driver name: ' + str(driver))

    elif db_config is not None and db_config.get('driver') is not None:
        return db_config['driver']  # type: ignore

    elif conn is not None:
        return _get_conn_driver_name(conn)

    elif output_format in ('polars', 'pandas'):
        return 'connectorx'

    else:
        if sync is None:
            raise Exception('must specify more driver parameters')
        if db_config is not None:
            dbms = db_config['dbms']
        elif uri is not None:
            dbms = conn_utils.parse_uri(uri)['dbms']
        else:
            raise Exception('must specify more parameters to determine driver')
        return _get_default_dbms_driver_name(dbms, sync=sync)


def _get_conn_driver_name(
    conn: spec.Connection | spec.AsyncConnection | str | spec.DBConfig,
) -> str:

    if isinstance(conn, (str, dict)):
        return 'connectorx'

    for driver_name in [
        'sqlite3',
        'aiosqlite',
        'psycopg',
    ]:
        module = sys.modules.get(driver_name)
        if module is not None:
            ConnectionType = getattr(module, 'Connection', None)
            if isinstance(ConnectionType, type) and isinstance(
                conn, ConnectionType
            ):
                return driver_name

            if driver_name == 'psycopg':
                if isinstance(conn, module.AsyncConnection):
                    return driver_name
    else:
        raise Exception('could not determine driver of conn')


def _get_default_dbms_driver_name(dbms: str, sync: bool) -> str:
    if dbms == 'sqlite':
        if sync:
            return 'sqlite3'
        else:
            return 'aiosqlite'
    elif dbms == 'postgresql':
        return 'psycopg'
    else:
        raise Exception('unknown dbms: ' + str(dbms))


def get_driver_class(
    driver: spec.DriverReference | None = None,
    *,
    sync: bool | None = None,
    output_format: spec.QueryOutputFormat | None = None,
    db_config: spec.DBConfig | None = None,
    uri: str | None = None,
    conn: spec.Connection
    | spec.AsyncConnection
    | str
    | spec.DBConfig
    | None = None,
) -> spec.DriverClass:

    if (
        driver is not None
        and isinstance(driver, type)
        and issubclass(driver, abstract_driver.AbstractDriver)
    ):
        return driver

    driver_name = get_driver_name(
        driver=driver,
        sync=sync,
        output_format=output_format,
        db_config=db_config,
        uri=uri,
        conn=conn,
    )

    if driver_name == 'sqlite3':
        from .driver_classes import sqlite3_driver

        return sqlite3_driver.Sqlite3Driver
    elif driver_name == 'aiosqlite':
        from .driver_classes import aiosqlite_driver

        return aiosqlite_driver.AiosqliteDriver
    elif driver_name == 'psycopg':
        from .driver_classes import psycopg_driver

        return psycopg_driver.PsycopgDriver
    elif driver_name == 'connectorx':
        from .driver_classes import connectorx_driver

        return connectorx_driver.ConnectorxDriver
    else:
        raise Exception('unknown driver: ' + str(driver))

