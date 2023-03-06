from __future__ import annotations

import typing

from toolsql import spec

if typing.TYPE_CHECKING:
    import contextlib
    import types

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

    conn: spec.Connection

    def __init__(self, conn: spec.Connection) -> None:
        self.conn = conn

        if spec.is_sqlite3_connection(self.conn):
            conn.execute('BEGIN')
        elif spec.is_psycopg_sync_connection(self.conn):
            pass
        else:
            raise Exception('invalid driver')

    def __enter__(self) -> None:
        pass

    def __exit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: types.TracebackType | None,
    ) -> None:

        if spec.is_sqlite3_connection(self.conn):
            if exception_type is None:
                self.conn.execute('COMMIT')
        elif spec.is_psycopg_sync_connection(self.conn):
            pass
        else:
            raise Exception('invalid driver')


class _AsyncTransactionContext:

    conn: spec.AsyncConnection

    def __init__(self, conn: spec.AsyncConnection) -> None:
        self.conn = conn

    async def __aenter__(self) -> None:
        if spec.is_aiosqlite_connection(self.conn):
            await self.conn.execute('BEGIN')
        elif spec.is_psycopg_async_connection(self.conn):
            pass
        else:
            raise Exception('invalid driver')

    async def __aexit__(
        self,
        exception_type: type[BaseException] | None,
        exception_value: BaseException | None,
        exception_traceback: types.TracebackType | None,
    ) -> None:
        if spec.is_aiosqlite_connection(self.conn):
            if exception_type is None:
                await self.conn.execute('COMMIT')
        elif spec.is_psycopg_async_connection(self.conn):
            pass
        else:
            raise Exception('invalid driver')


def transaction(
    conn: spec.Connection,
) -> _TransactionContext | contextlib._GeneratorContextManager[
    psycopg.Transaction
]:
    if spec.is_sqlite3_connection(conn):
        return _TransactionContext(conn=conn)
    elif spec.is_psycopg_sync_connection(conn):
        return conn.transaction()
    else:
        raise Exception('invalid driver')


def begin(conn: spec.Connection) -> None:
    if spec.is_sqlite3_connection(conn):
        conn.execute('BEGIN')
    elif spec.is_psycopg_sync_connection(conn):
        conn.execute('BEGIN')
    else:
        raise Exception('invalid driver')


def commit(conn: spec.Connection) -> None:

    if spec.is_sqlite3_connection(conn):
        conn.execute('COMMIT')
    elif spec.is_psycopg_sync_connection(conn):
        conn.commit()
    else:
        raise Exception('invalid driver')


def rollback(conn: spec.Connection) -> None:

    if spec.is_sqlite3_connection(conn):
        conn.execute('ROLLBACK')
    elif spec.is_psycopg_sync_connection(conn):
        conn.rollback()
    else:
        raise Exception('invalid driver')


#
# # async versions
#


def async_transaction(
    conn: spec.AsyncConnection,
) -> _AsyncTransactionContext | contextlib._AsyncGeneratorContextManager[
    psycopg.AsyncTransaction
]:
    if spec.is_aiosqlite_connection(conn):
        return _AsyncTransactionContext(conn=conn)
    elif spec.is_psycopg_async_connection(conn):
        return conn.transaction()
    else:
        raise Exception('invalid driver')


async def async_begin(conn: spec.AsyncConnection) -> None:
    if spec.is_aiosqlite_connection(conn):
        await conn.execute('BEGIN')
    elif spec.is_psycopg_async_connection(conn):
        await conn.execute('BEGIN')
    else:
        raise Exception('invalid driver')


async def async_commit(conn: spec.AsyncConnection) -> None:

    if spec.is_aiosqlite_connection(conn):
        await conn.execute('COMMIT')
    elif spec.is_psycopg_async_connection(conn):
        await conn.commit()
    else:
        raise Exception('invalid driver')


async def async_rollback(conn: spec.AsyncConnection) -> None:

    if spec.is_aiosqlite_connection(conn):
        await conn.execute('ROLLBACK')
    elif spec.is_psycopg_async_connection(conn):
        await conn.rollback()
    else:
        raise Exception('invalid driver')

