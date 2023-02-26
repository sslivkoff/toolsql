from __future__ import annotations

from toolsql import drivers
from toolsql import executors
from toolsql import spec


def get_conn_dialect(
    conn: spec.Connection | spec.AsyncConnection | str | spec.DBConfig,
) -> spec.DBMS:

    if isinstance(conn, str):
        if conn.startswith('postgres'):
            return 'postgresql'
        elif conn.startswith('sqlite'):
            return 'sqlite'
        else:
            raise Exception('unknown dialect')

    elif isinstance(conn, dict):
        dbms = conn['dbms']
        if dbms in ('postgresql', 'sqlite'):
            return dbms
        else:
            raise Exception('unknown dialect')

    else:
        driver = drivers.get_driver_name(conn=conn)
        if driver in ['psycopg']:
            return 'postgresql'
        elif driver in ['sqlite3', 'aiosqlite']:
            return 'sqlite'
        elif driver == 'connectorx':
            if not isinstance(conn, str):
                raise Exception('not a connectorx conn')
            if conn.startswith('sqlite'):
                return 'sqlite'
            elif conn.startswith('postgres'):
                return 'postgresql'
        raise Exception('unknown driver name')


def get_conn_db_name(conn: spec.Connection) -> str | None:
    dialect = get_conn_dialect(conn)
    if dialect == 'sqlite':
        return None
    elif dialect == 'postgresql':
        sql = 'SELECT current_database()'
        name: str = executors.raw_select(
            sql=sql, conn=conn, output_format='cell'
        )
        return name
    else:
        raise Exception('unknown dialect: ' + str(dialect))

