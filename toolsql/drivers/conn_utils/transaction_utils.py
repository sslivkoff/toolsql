from __future__ import annotations

import typing

from toolsql import spec
from .. import driver_utils

if typing.TYPE_CHECKING:
    import psycopg

# """

# 1.
#     with toolsql.connect(db_config) as conn:
#         with toolsql.begin(conn):
#             ...
#             toolsql.rollback(conn)

# 2.
#     conn = toolsql.connect(db_config)
#     with toolsql.begin(conn):
#         ...
#         toolsql.rollback(conn)

# 3.
#     conn = toolsql.connect(db_config)
#     toolsql.begin(conn)
#     ...
#     toolsql.commit(conn) | toolsql.rollback(conn)

# 4.
#     with toolsql.connect(db_config) as conn:
#         toolsql.begin(conn)
#         ...
#         toolsql.commit(conn) | toolsql.rollback(conn)


# """


class _TransactionContext:
    def __init__(self, conn: spec.Connection) -> None:
        self.conn = conn
        self.driver = driver_utils.get_driver_name(conn=conn)

        if self.driver == 'sqlite3':
            conn.execute('BEGIN')
        elif self.driver in ['psycopg']:
            pass
        else:
            raise Exception('invalid driver')

    def __enter__(self):
        pass

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if self.driver == 'sqlite3':
            if exception_type is None:
                self.conn.execute('COMMIT')


class _AsyncTransactionContext:
    def __init__(self, conn: spec.Connection) -> None:
        self.conn = conn
        self.driver = driver_utils.get_driver_name(conn=conn)

        if self.driver == 'aiosqlite':
            pass
        elif self.driver in ['psycopg']:
            pass
        else:
            raise Exception('invalid driver')

    async def __aenter__(self):
        if self.driver == 'aiosqlite':
            await self.conn.execute('BEGIN')
        elif self.driver in ['psycopg']:
            pass
        else:
            raise Exception('invalid driver')

    async def __aexit__(
        self, exception_type, exception_value, exception_traceback
    ):
        if self.driver == 'aiosqlite':
            if exception_type is None:
                await self.conn.execute('COMMIT')


def begin(conn: spec.Connection) -> _TransactionContext | psycopg.Transaction:
    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'sqlite3':
        return _TransactionContext(conn=conn)
    elif driver == 'psycopg':
        return conn.transaction()
    else:
        raise Exception('invalid driver')


def commit(conn: spec.Connection) -> None:

    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'sqlite3':
        conn.execute('BEGIN')
    elif driver == 'psycopg':
        conn.commit()
    else:
        raise Exception('invalid driver')


def rollback(conn: spec.Connection) -> None:

    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'sqlite3':
        conn.execute('ROLLBACK')
    elif driver == 'psycopg':
        conn.rollback()
    else:
        raise Exception('invalid driver')


#
# # async versions
#

def async_begin(
    conn: spec.Connection,
) -> _AsyncTransactionContext | psycopg.Transaction:
    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'aiosqlite':
        return _AsyncTransactionContext(conn=conn)
    elif driver == 'psycopg':
        return conn.transaction()
    else:
        raise Exception('invalid driver')


async def async_commit(conn: spec.Connection) -> None:

    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'sqlite3':
        await conn.execute('COMMIT')
    elif driver == 'psycopg':
        await conn.commit()
    else:
        raise Exception('invalid driver')


async def async_rollback(conn: spec.Connection) -> None:

    driver = driver_utils.get_driver_name(conn=conn)
    if driver == 'sqlite3':
        await conn.execute('ROLLBACK')
    elif driver == 'psycopg':
        await conn.rollback()
    else:
        raise Exception('invalid driver')

