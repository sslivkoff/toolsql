from __future__ import annotations

import sys

from toolsql import driver_utils
from toolsql import spec


def get_conn_driver(
    conn: spec.Connection | spec.AsyncConnection | str,
) -> spec.DriverClass:

    for driver_name in [
        'sqlite3',
        'aiosqlite',
        'psycopg',
        'connectorx',
    ]:
        module = sys.modules.get(driver_name)
        if module is not None:
            if driver_name == 'connectorx':
                if isinstance(conn, str):
                    return driver_utils.get_driver_class('connectorx')
            else:
                ConnectionType = getattr(module, 'Connection', None)
                if isinstance(ConnectionType, type) and isinstance(
                    conn, ConnectionType
                ):
                    return driver_utils.get_driver_class(driver_name)
    else:
        raise Exception('could not determine driver of conn')


def get_conn_dialect(
    conn: spec.Connection | spec.AsyncConnection | str,
) -> spec.DatabaseSystem:

    if isinstance(conn, str):
        if conn.startswith('postgres'):
            return 'postgresql'
        elif conn.startswith('sqlite'):
            return 'sqlite'
        else:
            raise Exception('unknown dialect')

    else:
        driver = get_conn_driver(conn)
        if driver.name in ['psycopg']:
            return 'postgresql'
        elif driver.name in ['sqlite3', 'aiosqlite']:
            return 'sqlite'
        elif driver.name == 'connectorx':
            if not isinstance(conn, str):
                raise Exception('not a connectorx conn')
            if conn.startswith('sqlite'):
                return 'sqlite'
            elif conn.startswith('postgres'):
                return 'postgresql'
        raise Exception('unknown driver name')

