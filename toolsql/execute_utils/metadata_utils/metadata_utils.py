from __future__ import annotations

import typing

from toolsql import conn_utils
from toolsql import spec
from . import dbms_utils


def get_table_schemas(
    conn: spec.Connection,
) -> typing.Mapping[str, spec.TableSchema]:
    dialect = conn_utils.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_table_schemas(conn=conn)


def get_table_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = conn_utils.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_tables_names(conn=conn)


def get_indices_names(conn: spec.Connection) -> typing.Sequence[str]:
    dialect = conn_utils.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_indices_names(conn=conn)


def get_table_schema(
    table_name: str, conn: spec.Connection
) -> spec.TableSchema:
    dialect = conn_utils.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_table_schema(table_name=table_name, conn=conn)


def get_table_create_statement(
    table_name: str, *, conn: spec.Connection
) -> str:
    dialect = conn_utils.get_conn_dialect(conn)
    dbms = dbms_utils.get_dbms_class(name=dialect)
    return dbms.get_table_create_statement(table_name=table_name, conn=conn)

