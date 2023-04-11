from __future__ import annotations

import typing

from . import typedata

if typing.TYPE_CHECKING:
    from typing_extensions import TypeGuard

    import sqlite3
    import aiosqlite
    import psycopg

    import pandas as pd  # type: ignore
    import polars as pl

    from . import typedefs


#
# # columntypes
#


def is_sqlite_columntype(
    item: typing.Any,
) -> TypeGuard[typedefs.SqliteColumntype]:
    return item in typedata.sqlite_columntypes


def is_postgresql_columntype(
    item: typing.Any,
) -> TypeGuard[typedefs.PostgresqlColumntype]:
    return item in typedata.postgresql_columntypes


def is_generic_columntype(
    item: typing.Any,
) -> TypeGuard[typedefs.GenericColumntype]:
    return item in typedata.generic_columntypes


#
# # dataframes
#


def is_pandas_dataframe(
    item: typing.Any,
) -> TypeGuard[pd.DataFrame]:
    item_type = type(item)
    return (
        item_type.__name__ == 'DataFrame'
        and item_type.__module__ == 'pandas.core.frame'
    )


def is_polars_dataframe(
    item: typing.Any,
) -> TypeGuard[pl.DataFrame]:
    item_type = type(item)
    return (
        item_type.__name__ == 'DataFrame'
        and item_type.__module__ in [
            'polars.internals.dataframe.frame',
            'polars.dataframe.frame',
        ]
    )


#
# # connections
#


def is_sqlite3_connection(
    item: typing.Any,
) -> TypeGuard[sqlite3.Connection]:
    item_type = type(item)
    return (
        item_type.__name__ == 'Connection' and item_type.__module__ == 'sqlite3'
    )


def is_aiosqlite_connection(
    item: typing.Any,
) -> TypeGuard[aiosqlite.Connection]:
    item_type = type(item)
    return (
        item_type.__name__ == 'Connection'
        and item_type.__module__ == 'aiosqlite.core'
    )


def is_psycopg_sync_connection(
    item: typing.Any,
) -> TypeGuard[psycopg.Connection[typing.Any]]:
    item_type = type(item)
    return (
        item_type.__name__ == 'Connection' and item_type.__module__ == 'psycopg'
    )


def is_psycopg_async_connection(
    item: typing.Any,
) -> TypeGuard[psycopg.AsyncConnection[typing.Any]]:
    item_type = type(item)
    return (
        item_type.__name__ == 'AsyncConnection'
        and item_type.__module__ == 'psycopg'
    )


def is_sync_connection(
    item: typing.Any,
) -> TypeGuard[sqlite3.Connection | psycopg.Connection[typing.Any]]:
    return is_sqlite3_connection(item) or is_psycopg_sync_connection(item)


def is_async_connection(
    item: typing.Any,
) -> TypeGuard[aiosqlite.Connection | psycopg.AsyncConnection[typing.Any]]:
    return is_aiosqlite_connection(item) or is_psycopg_async_connection(item)

