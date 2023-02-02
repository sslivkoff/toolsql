from __future__ import annotations

import typing

from toolsql import dbs
from toolsql import drivers
from toolsql import spec


def get_table_schemas(
    conn: spec.Connection,
) -> typing.Mapping[str, spec.TableSchema]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_schemas(conn=conn)


def get_table_raw_column_types(
    table: str | spec.TableSchema, conn: spec.Connection | str | spec.DBConfig
) -> typing.Mapping[str, str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_raw_column_types(table=table, conn=conn)


async def async_get_table_raw_column_types(
    table: str | spec.TableSchema, conn: spec.AsyncConnection | str | spec.DBConfig
) -> typing.Mapping[str, str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return await db.async_get_table_raw_column_types(table=table, conn=conn)


def has_table(table: str | spec.TableSchema, conn: spec.Connection) -> bool:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.has_table(table=table, conn=conn)


def get_table_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_tables_names(conn=conn)


def get_indices_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_indices_names(conn=conn)


def get_table_schema(
    table: str | spec.TableSchema, conn: spec.Connection
) -> spec.TableSchema:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_schema(table=table, conn=conn)

