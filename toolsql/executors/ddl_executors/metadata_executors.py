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
    table_name: str, conn: spec.Connection | str | spec.DBConfig
) -> typing.Mapping[str, str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_raw_column_types(table_name=table_name, conn=conn)


def has_table(table_name: str, conn: spec.Connection) -> bool:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.has_table(table_name=table_name, conn=conn)


def get_table_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_tables_names(conn=conn)


def get_indices_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_indices_names(conn=conn)


def get_table_schema(
    table_name: str, conn: spec.Connection
) -> spec.TableSchema:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_schema(table_name=table_name, conn=conn)


def get_table_create_statement(
    table_name: str, *, conn: spec.Connection
) -> str:
    dialect = drivers.get_conn_dialect(conn)
    db = dbs.get_db_class(name=dialect)
    return db.get_table_create_statement(table_name=table_name, conn=conn)

