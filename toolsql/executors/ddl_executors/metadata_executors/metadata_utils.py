from __future__ import annotations

import typing

from toolsql import drivers
from toolsql import spec
from . import dbms_utils


def get_table_schemas(
    conn: spec.Connection,
) -> typing.Mapping[str, spec.TableSchema]:
    dialect = drivers.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_table_schemas(conn=conn)


def has_table(table_name: str, conn: spec.Connection) -> bool:
    dialect = drivers.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.has_table(table_name=table_name, conn=conn)


def get_table_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_tables_names(conn=conn)


def get_indices_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = drivers.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_indices_names(conn=conn)


def get_table_schema(
    table_name: str, conn: spec.Connection
) -> spec.TableSchema:
    dialect = drivers.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_table_schema(table_name=table_name, conn=conn)


def get_table_create_statement(
    table_name: str, *, conn: spec.Connection
) -> str:
    dialect = drivers.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_table_create_statement(table_name=table_name, conn=conn)

