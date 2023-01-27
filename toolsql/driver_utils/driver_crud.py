from __future__ import annotations

from toolsql import conn_utils
from .. import spec
from .drivers import abstract_driver


def get_driver_name(
    driver: spec.DriverReference | None = None,
    *,
    sync: bool | None = None,
    output_format: spec.QueryOutputFormat | None = None,
    db_config: spec.DBConfig | None = None,
    uri: str | None = None,
) -> str:

    if (
        driver is not None
        and isinstance(driver, type)
        and issubclass(driver, abstract_driver.AbstractDriver)
    ):
        return driver.name

    elif isinstance(driver, str):
        return driver

    elif db_config is not None and db_config.get('driver') is not None:
        return db_config['driver']

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
    )

    if driver_name == 'sqlite3':
        from .drivers import sqlite3_driver

        return sqlite3_driver.Sqlite3Driver
    elif driver_name == 'aiosqlite':
        from .drivers import aiosqlite_driver

        return aiosqlite_driver.AiosqliteDriver
    elif driver_name == 'psycopg':
        from .drivers import psycopg_driver

        return psycopg_driver.PsycopgDriver
    elif driver_name == 'connectorx':
        raise Exception('connectorx has no driver class')
    else:
        raise Exception('unknown driver: ' + str(driver))

